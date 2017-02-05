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

const (
	created = iota
	running = iota
	running_backpressured = iota
)

func init()  {
	Log.SetHandler(log15.DiscardHandler())
}

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
	// Downstream(handleMessages) circuit breaker
	Name string
	Timeout                int `mapstructure:"timeout", json:"timeout"`
	RequestVolumeThreshold int `mapstructure:"request_volume_threshold", json:"request_volume_threshold"`
	ErrorPercentThreshold  int `mapstructure:"error_percent_threshold", json:"error_percent_threshold"`
	// SleepWindow will be adjusted to min(SleepWindow, BackoffConstWindow)
	SleepWindow            int `mapstructure:"sleep_window", json:"sleep_window"`

	// Upstream(SqsReceiveService) back-off pattern, which firstly grows
	// exponentially and then remains constant. BackoffExpInit and
	// BackoffConstWindow are in millisecond.
	// With values:
	// {
	// 	BackoffExpInit: 500
	// 	BackoffExpSteps: 5
	// 	BackoffConstWindow: 10000
	// }
	// SqsReceiveService will back off after these intervals in milliseconds:
	// 500, 1000, 2000, 4000, 8000, 10000, 10000, 10000, ...
	BackoffExpInit int
	BackoffExpSteps int
	BackoffConstWindow int
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

type BackOff interface {
	Run(work func() error) error
}

type BackoffImpl struct {
	expBackoff []time.Duration
	constBackoff []time.Duration
}

func NewBackoff(expInit int, expSteps int, constWindow int) BackOff {
	return &BackoffImpl{
		expBackoff: retrier.ExponentialBackoff(expSteps,
			time.Duration(expInit)*time.Millisecond),
		constBackoff: retrier.ConstantBackoff(1024,
			time.Duration(constWindow)*time.Millisecond),
	}
}

func (b *BackoffImpl) Run(work func() error) error {
	retryExp := retrier.New(b.expBackoff, nil)
	if err := retryExp.Run(work); err == nil {
		return nil
	}
	for {
		retryConst := retrier.New(b.constBackoff, nil)
		if err := retryConst.Run(work); err == nil {
			break
		}
	}
	return nil
}

// Two circuit breakers are there for back pressure:
// 1. Upstream(SqsReceiveService) breaker could be opened by the event that the
//    number of failed sqs.ReceiveMessage() passed a threshold.
// 2. Downstream breaker could be opened by the event that the number of failed
//    commands issued by downstream services passed a threshold.
// Either one of the breakers become open can trigger backoff of upstream.
// Whenever a backoff.Run() succeeded, which implies two breakers are both
// closed, the backoffCount is restored.
func (q *SqsReceiveService) backPressuredRun(downstreamCircuit *hystrix.CircuitBreaker, backoff BackOff) {
	for {
		backoffCount := 0
		q.log.Debug("[*run*] BackOff restored", "backoffCount", backoffCount)
		backoff.Run(func() error {
			backoffCount++
			q.log.Debug("[run] ReceiveMessage", "backoffCount", backoffCount)
			var msgs []*sqs.Message
			// The circuit protects reads from the upstream(sqs).
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
				// Enqueuing might block
				q.queue <- msg
			}
			if err != nil {
				// Could be the circuit is still opened or
				// sqs.ReceiveMessage() failed. Will be
				// retried at a backoff period.
				q.log.Warn("[run] Retry due to err", "err", err)
				return err
			}
			// The circuit for upstream at this point is ok.

			// However, the circuit for downstream service might
			// be calling for backing off.
			if downstreamCircuit.IsOpen() {
				q.log.Warn("[run] BackOff")
				return ErrBackoff
			}
			return nil
		})
	}
}

type MessageHandler func(*sqs.Message) error

func (q *SqsReceiveService) handleMessages(downstreamName string, handler MessageHandler) {
	// Spawn no more than q.Conf.MaxWaitingMessages goroutines
	semaphore := make(chan *struct{}, q.Conf.MaxWaitingMessages)
	for {
		q.log.Debug("[handler] Dequeuing")
		m := <- q.queue
		semaphore <- &struct{}{}
		q.log.Debug("[handler] Semophore got", "sem_len", len(semaphore))
		done := make(chan *struct{}, 1)
		errChan := hystrix.Go(downstreamName, func() error {
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

		// SleepWindow should be less than BackoffConstWindow
		sleepWindow := func() int {
			if bp.SleepWindow > bp.BackoffConstWindow {
				q.log.Warn("[run] bp.SleepWindow adjusted to be bp.BackoffConstWindow")
				return bp.BackoffConstWindow
			}
			return bp.SleepWindow
		}()
		// circuit for downstream
		hystrix.ConfigureCommand(bp.Name, hystrix.CommandConfig{
			MaxConcurrentRequests: q.Conf.MaxWaitingMessages,
			Timeout: bp.Timeout,
			RequestVolumeThreshold: bp.RequestVolumeThreshold,
			ErrorPercentThreshold: bp.ErrorPercentThreshold,
			SleepWindow: sleepWindow,
		})
		cb, _, _ := hystrix.GetCircuit(bp.Name)
		backoff := NewBackoff(bp.BackoffExpInit, bp.BackoffExpSteps,
			bp.BackoffConstWindow)

		go q.backPressuredRun(cb, backoff)
		go q.handleMessages(bp.Name, handler)
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

