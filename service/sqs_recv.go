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
	"github.com/inconshreveable/log15"
)

var ErrQueueTimeout = errors.New("See no message until timeout")
var ErrBackoff = errors.New("Should back off for a while")

var Log = log15.New()

func init()  {
	Log.SetHandler(log15.DiscardHandler())
}

const (
	created = iota
	running = iota
	running_backpressured = iota
)

type SqsReceiveService struct {
	Name string
	Conf SqsReceiveServiceConfig

	log log15.Logger
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

type RSpec interface {
	ToReceiveMessageInput(url string) (*sqs.ReceiveMessageInput, error)
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

func (spec *ReceiveSpec) ToReceiveMessageInput(url string) (*sqs.ReceiveMessageInput, error) {
	input := &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(url),
		AttributeNames: spec.AttributeNames,
		MessageAttributeNames: spec.MessageAttributeNames,
		MaxNumberOfMessages: spec.MaxNumberOfMessages,
		ReceiveRequestAttemptId: spec.ReceiveRequestAttemptId,
		VisibilityTimeout: spec.VisibilityTimeout,
		WaitTimeSeconds: spec.WaitTimeSeconds,
	}
	err := input.Validate()
	if err != nil {
		Log.Error("Validate failed", "err", err)
		return nil, err
	}
	return input, nil
}


type BackPressureConf struct {
	Name string
	Timeout                int `mapstructure:"timeout", json:"timeout"`
	RequestVolumeThreshold int `mapstructure:"request_volume_threshold", json:"request_volume_threshold"`
	ErrorPercentThreshold  int `mapstructure:"error_percent_threshold", json:"error_percent_threshold"`
	SleepWindow            int `mapstructure:"sleep_window", json:"sleep_window"`

	// The retry(backoff) pattern firstly grows exponential and then
	// remains constant.
	BackoffExpSteps int
	BackoffExpInit time.Duration
	BackoffConstSteps int
}

func (conf *SqsReceiveServiceConfig) createReceiveMessageInput(spec RSpec) (*sqs.ReceiveMessageInput, error) {
	input, err := spec.ToReceiveMessageInput(conf.Url)
	if err != nil {
		return nil, err
	}
	return input, nil
}

func (conf *SqsReceiveServiceConfig) NewSqsReceiveService(name string, client sqsiface.SQSAPI, spec RSpec) (*SqsReceiveService, error) {
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
	input, err := conf.createReceiveMessageInput(spec)
	if err != nil {
		return nil, err
	}
	return &SqsReceiveService {
		Name: name,
		Conf: *conf,
		log: Log.New("service", name),
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
	retry := retrier.New(retrier.ExponentialBackoff(bp.BackoffExpSteps,
		bp.BackoffExpInit), nil)
	retry_count := 0
	q.log.Debug("[*run*] Retrier restored", "retry_count", retry_count)
	for {
		// Every Run() starts a fresh counter.
		err := retry.Run(func() error {
			//for i := 1; i <= 3; i++ {
			//	j := retry.CalcSleep(i)
			//	q.log.Debug("[run]", "sleep", j)
			//}
			retry_count++
			q.log.Debug("[run] ReceiveMessage", "retry_count", retry_count)
			// The circuit protects reads from the upstream(sqs).
			var msgs []*sqs.Message
			err := hystrix.Do(q.Name, func() error {
				resp, err := q.client.ReceiveMessage(q.msg_input)
				if err != nil {
					q.log.Error("[run] ReceiveMessage err", "err", err)
					return err
				}
				q.log.Debug("[run] ReceiveMessage ok", "len", len(resp.Messages))
				msgs = resp.Messages
				return nil
			}, nil)
			for _, msg := range msgs {
				// enqueuing might block
				q.queue <- msg
			}
			if err != nil {
				// Could be the circuit is still opened or
				// sqs.ReceiveMessage() failed. Will be
				// retried later.
				q.log.Warn("[run] Retry due to err", "err", err)
				return err
			}

			// The circuit for upstream is ok. But the
			// downstream service might be calling for backing off.
			// This behaviour is in essence back pressure.
			if cb.IsOpen() {
				q.log.Warn("[run] Backoff")
				return ErrBackoff
			}
			return nil
		})

		if err != nil {
			// Failed every retry, a circuit breaker(either for
			// upstream or downstream) hasn't been restored.
			// Replace ExponentialBackoff or prolong ConstantBackoff.
			q.log.Warn("[run] Extending backoff")
			retry = retrier.New(
				retrier.ConstantBackoff(bp.BackoffConstSteps,
					time.Duration(bp.SleepWindow)*time.Millisecond),
				nil)
		} else {
			// A success would restore to ExponentialBackpoff again.
			q.log.Debug("[*run*] Retrier restored", "retry_count", retry_count)
			retry = retrier.New(retrier.ExponentialBackoff(bp.BackoffExpSteps,
				bp.BackoffExpInit), nil)
			retry_count = 0
		}
	}
}

type MessageHandler func(*sqs.Message) error


func (q *SqsReceiveService) handleMessages(bp *BackPressureConf, handler MessageHandler) {
	// Spawn no more than q.Conf.MaxWaitingMessages goroutines
	semaphore := make(chan *struct{}, q.Conf.MaxWaitingMessages)
	for {
		q.log.Debug("[handler] Dequeuing")
		m := <- q.queue
		semaphore <- &struct{}{}
		q.log.Debug("[handler] Semophore got", "sem_len", len(semaphore))
		done := make(chan *struct{}, 1)
		errChan := hystrix.Go(bp.Name, func() error {
			err := handler(m)
			if err != nil {
				q.log.Error("[handler] err", "err", err)
				return err
			}
			q.log.Debug("[handler] ok")
			done <- &struct{}{}
			return nil
		}, func(err error) error {
			q.log.Warn("[handler] fallback", "err", err)
			return ErrBackoff
		})
		go func() {
			select {
			case <-done:
			case <-errChan:
			}
			<- semaphore
		}()
	}
}

func (q *SqsReceiveService) RunWithBackPressure(bp BackPressureConf, handler MessageHandler) {
	q.once.Do(func() {
		q.log.Info("RunWithBackPressure() called")
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
		q.log.Info("Run() called")
		q.state = running
		go func () {
			for {
				q.log.Debug("Try ReceiveMessage")
				var msgs []*sqs.Message
				hystrix.Do(q.Name, func() error {
					resp, err := q.client.ReceiveMessage(q.msg_input)
					if err != nil {
						q.log.Error("ReceiveMessage", "err", err)
						return err
					}
					q.log.Debug("ReceiveMessage", "len", len(resp.Messages))
					msgs = resp.Messages
					return nil
				}, func (err error) error {
					// Could be the circuit is still opened
					// or sqs.ReceiveMessage() failed.
					q.log.Warn("Fallback of ReceiveMessage", "err", err)
					return err
				})
				for _, msg := range msgs {
					// enqueuing might block
					q.queue <- msg
				}
				q.log.Debug("Done")
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

