// vim:fileencoding=utf-8
package service

import (
	"time"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/inconshreveable/log15"
)

type RateLimitedSender struct {
	Sender
	limiter RateLimit
	log log15.Logger
}

func NewRateLimitedSender(sender Sender, limiter RateLimit) *RateLimitedSender {
	return &RateLimitedSender{
		Sender:sender,
		limiter: limiter,
		log: sender.GetLogger(),
	}
}

func (s *RateLimitedSender) SendMessage(msg interface{},
	timeout time.Duration) (interface{}, error) {

	begin := time.Now()
	if !s.limiter.Wait(1, timeout) {
		s.log.Warn("[Sender] Wait failed", "mixin", "RateLimited",
			"err", ErrRateLimited)
		return nil, ErrRateLimited
	}
	end := time.Now()
	spent := end.Sub(begin)
	if spent >= timeout {
		return nil, ErrTimeout
	}
	return s.Sender.SendMessage(msg, timeout - spent)
}

type CircuitBreakerSender struct {
	Sender
	Monitor
	Name string
	log log15.Logger
}

func NewCircuitBreakerSender(name string, sender Sender,
	conf hystrix.CommandConfig) *CircuitBreakerSender {

	hystrix.ConfigureCommand(name, conf)
	log := sender.GetLogger()
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

func (s *CircuitBreakerSender) SendMessage(msg interface{},
	timeout time.Duration) (interface{}, error) {

	result := make(chan interface{}, 1)
	err := hystrix.Do(s.Name, func () (error) {
		resp, err := s.Sender.SendMessage(msg, timeout)
		if err != nil {
			s.log.Error("[Sender] SendMessage err",
				"mixin", "CircuitBreaker", "err", err)
			return err
		}
		s.log.Debug("[Sender] SendMessage ok",
			"mixin", "CircuitBreaker")
		result <- resp
		return nil
	}, nil)

	if err != nil {
		s.log.Warn("[Sender] Circuit err","mixin", "CircuitBreaker",
			"err", err)
		if err == hystrix.ErrTimeout {
			// hystrix.ErrTimeout doesn't interrupt SendMessage().
			// It just contributes to circuit's metrics.
			return <- result, nil
		}
		return nil, err
	}
	return <- result, nil
}

