// vim:fileencoding=utf-8
package service

import (
	"time"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"sync"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/inconshreveable/log15"
	"errors"
	"gopkg.in/eapache/go-resiliency.v1/retrier"
	"github.com/cfchou/porter/service"
	"fmt"
)

var ErrQueueTimeout = errors.New("See no message until timeout")
var ErrBackOff = errors.New("Should back off for a while")

const (
	created                = iota
	running                = iota
	running_back_pressured = iota
)

type BackOffImpl struct {
	expBackOff   []time.Duration
	constBackOff []time.Duration
}

func NewBackOffImpl(expInit int, expSteps int, constWindow int) service.BackOff {
	return &BackOffImpl{
		expBackOff: retrier.ExponentialBackoff(expSteps,
			time.Duration(expInit)*time.Millisecond),
		constBackOff: retrier.ConstantBackoff(1024,
			time.Duration(constWindow)*time.Millisecond),
	}
}

func (b *BackOffImpl) Run(work func() error) error {
	retryExp := retrier.New(b.expBackOff, nil)
	if err := retryExp.Run(work); err == nil {
		return nil
	}
	for {
		retryConst := retrier.New(b.constBackOff, nil)
		if err := retryConst.Run(work); err == nil {
			break
		}
	}
	return nil
}

type ReceiveInput struct {
	sqs.ReceiveMessageInput
	log log15.Logger
}

func (spec *ReceiveInput) ToReceiveMessageInput() (*sqs.ReceiveMessageInput, error) {
	input := spec.ReceiveMessageInput
	err := input.Validate()
	if err != nil {
		spec.log.Error("Validate failed", "err", err)
		return nil, err
	}
	return &input, nil
}

type SqsUpStreamConf struct {
	// MaxWaitingMessages is the max number of messages that have not been
	// removed by WaitMessage(). It's the capacity of the buffered channel
	// SqsService.queue.
	MaxWaitingMessages int `mapstructure:"max_waiting_messages", json:"max_waiting_messages"`

	// RequestVolumeThreshold is the minimum number of requests in the past 10
	// seconds needed before the failed rate calculation kicks in.
	// The failed rate calculation is based on the number of of requests in
	// the past 10 seconds.
	RequestVolumeThreshold int `mapstructure:"request_volume_threshold", json:"request_volume_threshold"`

	// The circuit is opened if the railed rate reaches
	// ErrorPercentThreshold.
	ErrorPercentThreshold int `mapstructure:"error_percent_threshold", json:"error_percent_threshold"`

	// SleepWindow is the minimum time of how long, in milliseconds, to wait
	// after the circuit opens before testing for recovery
	SleepWindow int `mapstructure:"sleep_window", json:"sleep_window"`
}

type SqsUpStream struct {
	Name string
	Conf *SqsUpStreamConf

	log       log15.Logger
	client    sqsiface.SQSAPI
	queue     chan *sqs.Message
	msg_input *sqs.ReceiveMessageInput
	once      sync.Once

	// For back-pressure
	monitor service.DownStreamMonitor
	backOff service.BackOff
	state   int
}

func NewSqsUpStream(name string, conf SqsUpStreamConf, client sqsiface.SQSAPI, spec ReceiveSpec) (*SqsUpStream, error) {
	// Register a circuit breaker for sqs.ReceiveMessage()
	hystrix.ConfigureCommand(name, hystrix.CommandConfig{
		// Long polling is supported by sqs. A valid WaitTimeSeconds
		// is from 0 to 20. Set Timeout to be larger than 20 to avoid
		// clashing with WaitTimeSeconds.
		Timeout: 30000,
		// This command is run exclusively by this service and
		// sqs.ReceiveMessage() is always called sequentially.
		MaxConcurrentRequests:  1,
		RequestVolumeThreshold: conf.RequestVolumeThreshold,
		ErrorPercentThreshold:  conf.ErrorPercentThreshold,
		SleepWindow:            conf.SleepWindow,
	})
	input, err := spec.ToReceiveMessageInput()
	if err != nil {
		return nil, err
	}
	return &SqsUpStream{
		Name:      name,
		Conf:      &conf,
		log:       Log.New("service", name),
		client:    client,
		queue:     make(chan *sqs.Message, conf.MaxWaitingMessages),
		msg_input: input,
		state:     created,
	}, nil
}

func (up *SqsUpStream) SetBackPressure(monitor service.DownStreamMonitor, backOff service.BackOff) error {
	if up.state != created {
		panic("UpStream is already running")
	}
	up.monitor = monitor
	up.backOff = backOff
	return nil
}

func (up *SqsUpStream) WaitMessage(timeout time.Duration) (interface{}, error) {
	if timeout == 0 {
		m := <-up.queue
		return m, nil
	} else {
		tm := time.After(timeout)
		select {
		case m := <-up.queue:
			return m, nil
		case <-tm:
			return nil, ErrQueueTimeout
		}
	}
}

func (up *SqsUpStream) Run() {
	up.once.Do(func() {
		if up.backOff != nil && up.monitor != nil {
			up.state = running_back_pressured
			up.log.Info("[Up] Run, back pressured")
			up.backPressuredRun(up.monitor, up.backOff)
		} else {
			up.state = running
			up.log.Info("[Up] Run")
			up.normalRun()
		}
	})
}

func (up *SqsUpStream) backPressuredRun(monitor service.DownStreamMonitor, backOff service.BackOff) {
	backOffCount := 0
	up.log.Debug("[Up] BackOff restored", "backOffCount", backOffCount)
	for {
		err := backOff.Run(func() error {
			backOffCount++
			up.log.Debug("[Up] ReceiveMessage", "backOffCount", backOffCount)
			var msgs []*sqs.Message
			// The circuit protects reads from the upstream(sqs).
			err := hystrix.Do(up.Name, func() error {
				resp, err := up.client.ReceiveMessage(up.msg_input)
				if err != nil {
					up.log.Error("[Up] ReceiveMessage err", "err", err)
					return err
				}
				up.log.Debug("[Up] ReceiveMessage ok", "len", len(resp.Messages))
				msgs = resp.Messages
				return nil
			}, nil)
			for i, msg := range msgs {
				// Enqueuing might block
				nth := fmt.Sprintf("%d/%d", i+1, len(msgs))
				up.log.Debug("[Up] Enqueuing...", "nth/total", nth)
				up.queue <- msg
			}
			if err != nil {
				// Could be the circuit is still opened or
				// sqs.ReceiveMessage() failed. Will be
				// retried at a backoff period.
				up.log.Warn("[Up] Retry due to err", "err", err)
				return err
			}
			// The circuit for upstream at this point is ok.

			// However, the circuit for downstream service might
			// be calling for backing off.
			if monitor.NeedBackOff() {
				up.log.Warn("[Up] BackOff needed")
				return ErrBackOff
			}
			return nil
		})
		if err == nil {
			backOffCount = 0
			up.log.Debug("[Up] BackOff restored", "backOffCount", backOffCount)
		}
	}
}

func (up *SqsUpStream) normalRun() {
	for {
		up.log.Debug("[Up] Try ReceiveMessage")
		var msgs []*sqs.Message
		hystrix.Do(up.Name, func() error {
			resp, err := up.client.ReceiveMessage(up.msg_input)
			if err != nil {
				up.log.Error("[Up] ReceiveMessage err", "err", err)
				return err
			}
			up.log.Debug("[Up] ReceiveMessage ok", "len", len(resp.Messages))
			msgs = resp.Messages
			return nil
		}, func(err error) error {
			// Could be the circuit is still opened
			// or sqs.ReceiveMessage() failed.
			up.log.Warn("[Up] Fallback of ReceiveMessage", "err", err)
			return err
		})
		for _, msg := range msgs {
			// enqueuing might block
			up.queue <- msg
		}
		up.log.Debug("[Up] Done")
	}
}
