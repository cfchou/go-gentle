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
	lock sync.Mutex
	ticker *backoff.Ticker
	interval time.Duration
	log log15.Logger
	state int
}

// TODO mixed back-off: exponential back-off followed by constant back-off
func NewBackOffReceiver(receiver Receiver, monitor Monitor, interval time.Duration) *BackOffReceiver {
	return &BackOffReceiver{
		Receiver: receiver,
		monitor: monitor,
		ticker: backoff.NewTicker(&backoff.ZeroBackOff{}),
		interval: interval,
		log: receiver.Logger().New("mixin", "backoff"),
		state: normal_state,
	}
}

func (r *BackOffReceiver) ReceiveMessages() ([]Message, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.monitor.NeedBackOff() {
		if r.state == normal_state {
			r.state = backoff_state
			r.ticker.Stop()
			r.ticker = backoff.NewTicker(backoff.NewConstantBackOff(r.interval))
		}
	}
	<- r.ticker.C
	msgs, err := r.Receiver.ReceiveMessages()
	if err == nil {
		if r.state == backoff_state {
			r.state = normal_state
			r.ticker.Stop()
			r.ticker = backoff.NewTicker(&backoff.ZeroBackOff{})
		} else {
			r.log.Debug("[Receiver] ReceiveMessages ok","len", len(msgs))
		}
	} else {
		if r.state == normal_state {
			r.log.Error("[Receiver] ReceiveMessages err, start backing off", "err", err)
			r.state = backoff_state
			r.ticker.Stop()
			r.ticker = backoff.NewTicker(backoff.NewConstantBackOff(r.interval))
		} else {
			r.log.Error("[Receiver] ReceiveMessages err", "err", err)
		}
	}
	return msgs, err
}

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
