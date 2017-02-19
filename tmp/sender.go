// vim:fileencoding=utf-8
package service

import (
	"time"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/inconshreveable/log15"
)

type RateLimitedSender struct {
	Sender
	Name string
	limiter RateLimit
	log log15.Logger
}

func NewRateLimitedSender(name string, sender Sender, limiter RateLimit) *RateLimitedSender {
	return &RateLimitedSender{
		Sender:  sender,
		Name:    name,
		limiter: limiter,
		log: sender.Logger().New("mixin", "rate"),
	}
}

func (s *RateLimitedSender) SendMessage(msg Message, timeout time.Duration) (Message, error) {

	s.log.Debug("[Sender] SendMessage...", "msg", msg.Id())
	if timeout == 0 {
		s.limiter.Wait(1, timeout)
		s.log.Debug("[Sender] SendMessage ok","msg", msg.Id())
		return s.Sender.SendMessage(msg, timeout)
	}
	begin := time.Now()
	end_allowed := begin.Add(timeout)
	if !s.limiter.Wait(1, timeout) {
		s.log.Warn("[Sender] Wait failed","err", ErrRateLimited,
			"msg", msg.Id())
		return nil, ErrRateLimited
	}
	end := time.Now()
	if !end.Before(end_allowed) {
		s.log.Warn("[Sender] Too late","err", ErrTimeout,
			"msg", msg.Id())
		return nil, ErrTimeout
	}
	s.log.Debug("[Sender] SendMessage ok", "msg", msg.Id())
	return s.Sender.SendMessage(msg, end_allowed.Sub(end))
}

type CircuitBreakerSender struct {
	Sender
	Name string
	Monitor
	log log15.Logger
}

func NewCircuitBreakerSender(name string, sender Sender,
	conf hystrix.CommandConfig) *CircuitBreakerSender {

	hystrix.ConfigureCommand(name, conf)
	log := sender.Logger().New("mixin", "circuit")
	return &CircuitBreakerSender{
		Sender: sender,
		Monitor: &CircuitBreakerMonitor{
			Name: name,
			log: log,
			sleepWindow: conf.SleepWindow,
			openedOrLastTestedTime: time.Now().Unix(),
		},
		Name: name,
		log: log,
	}
}

func (s *CircuitBreakerSender) SendMessage(msg Message, timeout time.Duration) (Message, error) {

	result := make(chan interface{}, 1)
	s.log.Debug("[Sender] Circuit Do", "msg", msg.Id())
	err := hystrix.Do(s.Name, func () (error) {
		resp, err := s.Sender.SendMessage(msg, timeout)
		if err != nil {
			s.log.Error("[Sender] SendMessage err",
				"msg", msg.Id(), "err", err)
			result <- err
			return err
		}
		s.log.Debug("[Sender] SendMessage ok", "msg", msg.Id())
		result <- resp
		return nil
	}, nil)

	// hystrix.Do() is synchronous so at this point there are three
	// possibilities:
	// 1. work function is prevented from execution, err contains
	//    hystrix.ErrCircuitOpen or hystrix.ErrMaxConcurrency.
	// 2. work is finished before hystrix's timeout. err is nil or err
	//    returned by SendMessage().
	// 3. work is finished after hystrix's timeout. err is
	//    hystrix.ErrTimeout. There may be err from SendMessage() but
	//    it's overwritten.
	// In case 2 and 3 we'd like to return SendMessages()'s err if there's
	// any.
	// hystrix.ErrTimeout doesn't interrupt SendMessage().
	// It just contributes to circuit's metrics.

	if err != nil {
		s.log.Warn("[Sender] Circuit err","err", err,
			"msg", msg.Id())
		if err != hystrix.ErrTimeout {
			// Can be ErrCircuitOpen, ErrMaxConcurrency or
			// SendMessage()'s err.
			return nil, err
		}
	}
	switch v := <-result.(type) {
	case Message:
		return v, nil
	case error:
		return nil, v
	default:
		panic("Never be here")
	}
}

