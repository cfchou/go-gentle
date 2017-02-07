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

	s.log.Debug("[Sender] SendMessage...", "msg", msg.Id(),
		"when", time.Now().Unix())
	if timeout == 0 {
		s.limiter.Wait(1, timeout)
		s.log.Debug("[Sender] SendMessage ok","msg", msg.Id(),
			"when", time.Now().Unix())
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
	s.log.Debug("[Sender] SendMessage ok", "msg", msg.Id(),
		"when", time.Now().Unix())
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

	result := make(chan Message, 1)
	s.log.Debug("[Sender] Circuit Do", "msg", msg.Id(),
		"when", time.Now().Unix())
	err := hystrix.Do(s.Name, func () (error) {
		resp, err := s.Sender.SendMessage(msg, timeout)
		if err != nil {
			s.log.Error("[Sender] SendMessage err",
				"msg", msg.Id(), "err", err)
			return err
		}
		s.log.Debug("[Sender] SendMessage ok", "msg", msg.Id())
		result <- resp
		return nil
	}, nil)

	if err != nil {
		s.log.Warn("[Sender] Circuit err","err", err,
			"msg", msg.Id(), "when", time.Now().Unix())
		if err == hystrix.ErrTimeout {
			// hystrix.ErrTimeout doesn't interrupt SendMessage().
			// It just contributes to circuit's metrics.
			return <- result, nil
		}
		return nil, err
	}
	return <- result, nil
}

