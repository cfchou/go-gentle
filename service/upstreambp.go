// vim:fileencoding=utf-8
package service

import (
	"sync"
	"time"
	"sync/atomic"
	"github.com/afex/hystrix-go/hystrix"
	"fmt"
	"github.com/inconshreveable/log15"
)

type DefaultBackPressuredUpStream struct {
	Name string

	log                    log15.Logger
	state int32
	lock sync.Mutex

	client    Reader
	queue     chan Message

	// For back-pressure
	monitor Monitor
	backOff BackOff
}

func NewDefaultBackPressuredUpStream(name string, maxWaitingMessages int,
	client Reader, monitor Monitor, backOff BackOff) *DefaultBackPressuredUpStream {

	return &DefaultBackPressuredUpStream{
		Name:      name,
		log:       UpStreamLog.New("service", name),
		state: created,
		client:    client,
		queue:     make(chan Message, maxWaitingMessages),
		monitor: monitor,
		backOff: backOff,
	}
}

func (up *DefaultBackPressuredUpStream) WaitMessage(timeout time.Duration) (Message, error) {
	return WaitMessage(up.queue, timeout)
}

func (up *DefaultBackPressuredUpStream) Run() error {
	swapped := atomic.CompareAndSwapInt32(&up.state, created, running)
	if !swapped {
		return ErrRepeatedRun
	}
	up.log.Info("[Up] Run, back pressured")
	up.run(up.monitor, up.backOff)
	panic("Never come here")
	return nil
}

func (up *DefaultBackPressuredUpStream) run(monitor Monitor, backOff BackOff) {
	backOffCount := 0
	up.log.Debug("[Up] BackOff restored", "backOffCount", backOffCount)
	for {
		err := backOff.Run(func() error {
			backOffCount++
			up.log.Debug("[Up] ReceiveMessages", "backOffCount", backOffCount)
			var msgs []Message
			// The circuit protects reads from the upstream(sqs).
			err := hystrix.Do(up.Name, func() error {
				var err error
				msgs, err = up.client.ReceiveMessages()
				if err != nil {
					up.log.Error("[Up] ReceiveMessages err", "err", err)
					return err
				}
				up.log.Debug("[Up] ReceiveMessages ok", "len", len(msgs))
				return nil
			}, nil)
			for i, msg := range msgs {
				// Enqueuing might block
				nth := fmt.Sprintf("%d/%d", i+1, len(msgs))
				up.log.Debug("[Up] Enqueuing...",
					"nth/total", nth, "msg", msg.Id())
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
