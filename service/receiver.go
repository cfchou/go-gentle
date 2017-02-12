// vim:fileencoding=utf-8
package service

import (
	"sync"
	"github.com/cenkalti/backoff"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/inconshreveable/log15"
	"time"
)

const (
	normal_state = iota
	backoff_state = iota
)


// Note that BackOffReceiver is thread-safe by serializing ReceiveMessage().
// Safety is favoured over performance.
type BackOffReceiver struct {
	Receiver
	monitor Monitor

	log log15.Logger
	interval time.Duration

	lock sync.RWMutex
	ticker *backoff.Ticker
	state int
	last int64
}

// TODO mixed back-off: exponential back-off followed by constant back-off
func NewBackOffReceiver(receiver Receiver, monitor Monitor, interval time.Duration) *BackOffReceiver {
	return &BackOffReceiver{
		Receiver: receiver,
		monitor: monitor,
		log: receiver.Logger().New("mixin", "backoff"),
		interval: interval,
		ticker: backoff.NewTicker(&backoff.ZeroBackOff{}),
		state: normal_state,
		last: time.Now().UnixNano(),
	}
}

func (r *BackOffReceiver) ReceiveMessages() ([]Message, error) {

	need := r.monitor.NeedBackOff()
	if need {
		r.log.Warn("[Receiver] NeedBackOff")
		finish_at := time.Now().UnixNano()
		r.lock.Lock()
		if r.state == normal_state && finish_at > r.last {
			r.log.Warn("[Receiver] NeedBackOff normal -> backoff")
			r.state = backoff_state
			r.last = finish_at
			r.ticker.Stop()
			r.ticker = backoff.NewTicker(backoff.NewConstantBackOff(r.interval))
			// one tick is immediately presented when state changed
			<- r.ticker.C
		}
		<- r.ticker.C
		r.lock.Unlock()
	} else {
		r.lock.RLock()
		<- r.ticker.C
		r.lock.RUnlock()
	}
	msgs, err := r.Receiver.ReceiveMessages()
	finish_at := time.Now().UnixNano()
	go func() {
		// last-commit-wins if multiple calls run concurrently.
		r.lock.Lock()
		defer r.lock.Unlock()
		if err == nil {
			// favour normal_state when draw
			if r.state == backoff_state && finish_at >= r.last {
				r.log.Info("[Receiver] ReceiveMessages ok; backoff -> normal",
					"when", finish_at)
				r.state = normal_state
				r.ticker.Stop()
				r.ticker = backoff.NewTicker(&backoff.ZeroBackOff{})
				// one tick is immediately presented when state changed
				<- r.ticker.C
			} else {
				r.log.Debug("[Receiver] ReceiveMessages ok",
					"len", len(msgs), "when", finish_at)
			}
		} else {
			if r.state == normal_state && finish_at > r.last {
				r.log.Error("[Receiver] ReceiveMessages err; normal -> backoff",
					"err", err, "when", finish_at)
				r.state = backoff_state
				r.ticker.Stop()
				r.ticker = backoff.NewTicker(backoff.NewConstantBackOff(r.interval))
				// one tick is immediately presented when state changed
				<- r.ticker.C
			} else {
				r.log.Error("[Receiver] ReceiveMessages err", "err", err,
					"when", finish_at)
			}
		}
		if finish_at > r.last {
			r.last = finish_at
		}
	}()
	return msgs, err
}

/*
type RateLimitedReceiver struct {
	Receiver
	Name string
	limiter RateLimit
	log log15.Logger
}

func NewRateLimitedReceiver(name string, receiver Receiver, limiter RateLimit) *RateLimitedReceiver {
	return &RateLimitedReceiver{
		Receiver:  receiver,
		Name:    name,
		limiter: limiter,
		log: receiver.Logger().New("mixin", "rate"),
	}
}

func (r *RateLimitedReceiver) ReceiveMessages() ([]Message, error) {
	r.log.Debug("[Receiver] Wait...")
	r.limiter.Wait(1, 0)
	r.log.Debug("[Receiver] Waited")
	return r.Receiver.ReceiveMessages()
}
*/

type CircuitBreakerReceiver struct {
	Receiver
	Name string
	log log15.Logger
}

func NewCircuitBreakerReceiver(name string, receiver Receiver,
	conf hystrix.CommandConfig) *CircuitBreakerReceiver {

	hystrix.ConfigureCommand(name, conf)
	log := receiver.Logger().New("mixin", "circuit")
	return &CircuitBreakerReceiver{
		Receiver: receiver,
		Name: name,
		log: log,
	}
}

func (r *CircuitBreakerReceiver) ReceiveMessages() ([]Message, error) {

	result := make(chan *[]Message, 1)
	r.log.Debug("[Receiver] Circuit Do")
	err := hystrix.Do(r.Name, func() error {
		msgs, err := r.Receiver.ReceiveMessages()
		if err != nil {
			r.log.Error("[Receiver] ReceiveMessages err", "err", err)
			return err
		}
		r.log.Debug("[Receiver] ReceiveMessages ok", "len", len(msgs))
		result <- &msgs
		return nil
	}, nil)

	if err != nil {
		r.log.Warn("[Receiver] Circuit err","err", err)
		// hystrix.ErrTimeout doesn't interrupt ReceiveMessages().
		// It just contributes to circuit's metrics.
		if err != hystrix.ErrTimeout {
			return nil, err
		}
	}
	return *<- result, nil
}
