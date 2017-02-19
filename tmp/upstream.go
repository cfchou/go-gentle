// vim:fileencoding=utf-8
package service

import (
	"github.com/inconshreveable/log15"
	"time"
	"fmt"
	"sync/atomic"
)

var UpStreamLog = Log.New()

func WaitMessage(queue chan Message, timeout time.Duration) (Message, error) {
	if timeout == 0 {
		m := <- queue
		return m, nil
	} else {
		tm := time.After(timeout)
		select {
		case m := <- queue:
			return m, nil
		case <-tm:
			return nil, ErrTimeout
		}
	}
}

type DefaultUpStream struct {
	Name string

	log                    log15.Logger
	state int32

	receiver Receiver
	queue chan Message
}

func NewDefaultUpStream(name string, maxWaitingMessages int, receiver Receiver) *DefaultUpStream {
	return &DefaultUpStream{
		Name:      name,
		log:       UpStreamLog.New("service", name),
		state: created,
		//client:    client,
		receiver: receiver,
		queue:     make(chan Message, maxWaitingMessages),
	}
}

func (up *DefaultUpStream) WaitMessage(timeout time.Duration) (Message, error) {
	return WaitMessage(up.queue, timeout)
}

func (up *DefaultUpStream) Run() error {
	swapped := atomic.CompareAndSwapInt32(&up.state, created, running)
	if !swapped {
		return ErrRepeatedRun
	}
	up.log.Info("[Up] Run")
	up.run()
	panic("Never come here")
	return nil
}

func (up *DefaultUpStream) run() {
	for {
		msgs, err := up.receiver.ReceiveMessages()
		if err != nil {
			up.log.Error("[Up] ReceiveMessages err", "err", err)
			continue
		}
		for i, msg := range msgs {
			// Enqueuing might block
			nth := fmt.Sprintf("%d/%d", i+1, len(msgs))
			up.log.Debug("[Up] Enqueuing...",
				"nth/total", nth, "msg", msg.Id())
			up.queue <- msg
		}
		up.log.Debug("[Up] Done")
	}
}

/*
func (up *DefaultUpStream) run() {
	for {
		up.log.Debug("[Up] Try ReceiveMessages")
		var msgs []Message
		hystrix.Do(up.Name, func() error {
			var err error
			msgs, err := up.client.ReceiveMessages()
			if err != nil {
				up.log.Error("[Up] ReceiveMessages err", "err", err)
				return err
			}
			up.log.Debug("[Up] ReceiveMessages ok", "len", len(msgs))
			return nil
		}, func(err error) error {
			// Could be the circuit is still opened
			// or sqs.ReceiveMessage() failed.
			up.log.Warn("[Up] Fallback of ReceiveMessage", "err", err)
			return err
		})
		for i, msg := range msgs {
			// Enqueuing might block
			nth := fmt.Sprintf("%d/%d", i+1, len(msgs))
			up.log.Debug("[Up] Enqueuing...",
				"nth/total", nth, "msg", msg.Id())
			up.queue <- msg
		}
		up.log.Debug("[Up] Done")
	}
}
*/
