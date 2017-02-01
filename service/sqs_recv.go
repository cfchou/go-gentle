// vim:fileencoding=utf-8
package service

import (
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"sync"
	"github.com/afex/hystrix-go/hystrix"
	"time"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"gopkg.in/eapache/go-resiliency.v1/retrier"
)

var ErrQueueTimeout = errors.New("See no message until timeout")
var ErrBackoff = errors.New("Should back off for a while")

const (
	created = iota
	running = iota
	running_backpressured = iota
)

type SqsReceiveService struct {
	Name string
	Conf SqsReceiveServiceConfig

	client sqsiface.SQSAPI
	queue chan *sqs.Message
	msg_input *sqs.ReceiveMessageInput
	once sync.Once
	state int
}

type SqsReceiveServiceConfig struct {
	Url                    string `json:"url"`

	// MaxWaitingMessages is the max number of messages that have not been
	// removed by WaitMessage(). It's the capacity of the buffered channel
	// SqsService.queue.
	MaxWaitingMessages     int `mapstructure:"max_waiting_messages", json:"max_waiting_messages"`

	// RequestVolumeThreshold is the minimum number of requests in the past 10
	// seconds needed before the failed rate calculation kicks in.
	// The failed rate calculation is based on the number of of requests in
	// the past 10 seconds.
	RequestVolumeThreshold int `mapstructure:"request_volume_threshold", json:"request_volume_threshold"`

	// The circuit is opened if the railed rate reaches
	// ErrorPercentThreshold.
	ErrorPercentThreshold  int `mapstructure:"error_percent_threshold", json:"error_percent_threshold"`

	// SleepWindow is the minimum time of how long, in milliseconds, to wait
	// after the circuit opens before testing for recovery
	SleepWindow            int `mapstructure:"sleep_window", json:"sleep_window"`
}

// Almost identical to sqs.ReceiveMessageInput
type ReceiveSpec struct {
	AttributeNames []*string
	MessageAttributeNames []*string
	// Valid values are 1 to 10
	MaxNumberOfMessages *int64
	ReceiveRequestAttemptId *string
	VisibilityTimeout *int64
	// Valid values are 0 to 20
	WaitTimeSeconds *int64
}

type BackPressureConf struct {
	Name string
	Timeout                int `mapstructure:"timeout", json:"timeout"`
	RequestVolumeThreshold int `mapstructure:"request_volume_threshold", json:"request_volume_threshold"`
	ErrorPercentThreshold  int `mapstructure:"error_percent_threshold", json:"error_percent_threshold"`
	SleepWindow            int `mapstructure:"sleep_window", json:"sleep_window"`

	// The retry(backoff) pattern firstly grows exponential and then
	// remains constant.
	BackoffExpUnit int
	BackoffExpInit time.Duration
	BackoffConstUnit int
	BackoffConstInterval time.Duration
}

func (conf *SqsReceiveServiceConfig) createReceiveMessageInput(spec *ReceiveSpec) (*sqs.ReceiveMessageInput, error) {
	input := &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(conf.Url),
		AttributeNames: spec.AttributeNames,
		MessageAttributeNames: spec.MessageAttributeNames,
		MaxNumberOfMessages: spec.MaxNumberOfMessages,
		ReceiveRequestAttemptId: spec.ReceiveRequestAttemptId,
		VisibilityTimeout: spec.VisibilityTimeout,
		WaitTimeSeconds: spec.WaitTimeSeconds,
	}
	err := input.Validate()
	if err != nil {
		return nil, err
	}
	return input, nil
}

func (conf *SqsReceiveServiceConfig) NewSqsReceiveService(name string, client sqsiface.SQSAPI, spec ReceiveSpec) (*SqsReceiveService, error) {
	// Register a circuit breaker for sqs.ReceiveMessage()
	hystrix.ConfigureCommand(name, hystrix.CommandConfig{
		// Long polling is supported by sqs. A valid WaitTimeSeconds
		// is from 0 to 20. Set Timeout to be larger than 20 to avoid
		// clashing with WaitTimeSeconds.
		Timeout: 30000,
		// This command is run exclusively by this service and
		// sqs.ReceiveMessage() is always called sequentially.
		MaxConcurrentRequests: 1,
		RequestVolumeThreshold: conf.RequestVolumeThreshold,
		ErrorPercentThreshold: conf.ErrorPercentThreshold,
		SleepWindow: conf.SleepWindow,
	})
	input, err := conf.createReceiveMessageInput(&spec)
	if err != nil {
		return nil, err
	}
	return &SqsReceiveService {
		Name: name,
		Conf: *conf,
		client: client,
		queue: make(chan *sqs.Message, conf.MaxWaitingMessages),
		msg_input: input,
		state: created,
	}, nil
}

// Two circuit breakers are set up for back pressure:
// 1. Upstream breaker could be opened by the event that the number of failed
//    sqs.ReceiveMessage() passed a threshold.
// 2. Downstream breaker could be opened by the event that the number of failed
//    commands issued by downstream services passed a threshold.
// Either one of the breakers become open can trigger retry.
// The retry pattern firstly grows exponential and then remains constant.
// Whenever a retry succeeded, which implies two breakers are both closed, the
// pattern is restored.
func (q *SqsReceiveService) backPressuredRun(bp *BackPressureConf) {
	// The circuit whose state is controlled by the result of the downstream
	// service.
	cb, _, _ := hystrix.GetCircuit(bp.Name)
	retry := retrier.New(retrier.ExponentialBackoff(bp.BackoffExpUnit,
		bp.BackoffExpInit), nil)
	for {
		// Every Run() starts a fresh counter.
		err := retry.Run(func() error {
			// The circuit protects reads from the upstream(sqs).
			err := hystrix.Do(q.Name, func() error {
				resp, err := q.client.ReceiveMessage(q.msg_input)
				if err != nil {
					// log ReceiveMessage() failed
					return err
				}
				for _, msg := range resp.Messages {
					// enqueuing might block
					q.queue <- msg
				}
				return nil
			}, nil)
			if err != nil {
				// Could be the circuit is still opened or
				// sqs.ReceiveMessage() failed. Will be
				// retried later.
				return err
			}

			// The circuit for upstream is ok. But the
			// downstream service might be calling for backing off.
			// This behaviour is in essence back pressure.
			if !cb.AllowRequest() {
				return ErrBackoff
			}
			return nil
		})

		if err != nil {
			// Failed every retry, a circuit breaker(either for
			// upstream or downstream) hasn't been restored.
			// Replace ExponentialBackoff or prolong ConstantBackoff.
			retry = retrier.New(retrier.ConstantBackoff(
				bp.BackoffConstUnit,
				bp.BackoffConstInterval), nil)
		} else {
			// A success would restore to ExponentialBackpoff again.
			retry = retrier.New(retrier.ExponentialBackoff(8, 2),
				nil)
		}
	}
}

type MessageHandler func(*sqs.Message) error

func (q *SqsReceiveService) handleMessages(bp *BackPressureConf, handler MessageHandler) {
	semaphore := make(chan *struct{}, q.Conf.MaxWaitingMessages)
	for {
		m := <- q.queue
		// Spawn no more than q.Conf.MaxWaitingMessages goroutines
		semaphore <- &struct{}{}
		hystrix.Go(bp.Name, func() error {
			err := handler(m)
			if err != nil {
				// log
				return err
			}
			<- semaphore
			return nil
		}, func(err error) error {
			<- semaphore
			return err
		})
	}
}

func (q *SqsReceiveService) RunWithBackPressure(bp BackPressureConf, handler MessageHandler) {
	q.once.Do(func() {
		q.state = running_backpressured
		hystrix.ConfigureCommand(bp.Name, hystrix.CommandConfig{
			MaxConcurrentRequests: q.Conf.MaxWaitingMessages,
			Timeout: bp.Timeout,
			RequestVolumeThreshold: bp.RequestVolumeThreshold,
			ErrorPercentThreshold: bp.ErrorPercentThreshold,
			SleepWindow: bp.SleepWindow,
		})
		go q.backPressuredRun(&bp)
		go q.handleMessages(&bp, handler)
	})
}

func (q *SqsReceiveService) Run() {
	q.once.Do(func() {
		q.state = running
		go func () {
			for {
				hystrix.Do(q.Name, func() error {
					resp, err := q.client.ReceiveMessage(q.msg_input)
					if err != nil {
						// log ReceiveMessage() failed
						return err
					}
					for _, msg := range resp.Messages {
						// enqueuing might block
						q.queue <- msg
					}
					return nil
				}, func (err error) error {
					// Could be the circuit is still opened
					// or sqs.ReceiveMessage() failed.
					return err
				})
			}
		}()
	})
}

func (q *SqsReceiveService) WaitMessage(timeout time.Duration) (*sqs.Message, error) {
	if q.state != running {
		panic("Should have Run()")
	}
	if timeout == 0 {
		m := <- q.queue
		return m, nil
	} else {
		tm := time.After(timeout)
		select {
		case m := <- q.queue:
			return m, nil
		case <- tm:
			return nil, ErrQueueTimeout
		}
	}
}

