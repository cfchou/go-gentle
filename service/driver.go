// vim:fileencoding=utf-8
package service

import (
	"github.com/inconshreveable/log15"
	"time"
)

type RateLimitedDriver struct {
	Driver
	Name string
	limiter RateLimit
	log log15.Logger
}

func NewRateLimitedDriver(name string, channel Driver, limiter RateLimit) *RateLimitedDriver {
	return &RateLimitedDriver{
		Driver:  channel,
		Name:    name,
		limiter: limiter,
		log: channel.Logger().New("mixin", name),
	}
}

func (s *RateLimitedDriver) exchange(msg Message) (Messages, error) {
	s.limiter.Wait(1, 0)
	msgs, err := s.Driver.Exchange(msg, 0)
	if err != nil {
		s.log.Error("[Driver] Exchange err","err", err,
			"msg_in", msg.Id())
		return nil, err
	}
	s.log.Debug("[Driver] Exchange ok","msg_in", msg.Id(),
		"msg_out", msgs.Id())
	return msgs, nil
}

func (s *RateLimitedDriver) Exchange(msg Message, timeout time.Duration) (Messages, error) {
	s.log.Debug("[Driver] Exchange()", "msg_in", msg.Id(),
		"timeout", timeout)
	if timeout == 0 {
		return s.exchange(msg)
	}
	end_allowed := time.Now().Add(timeout)
	if !s.limiter.Wait(1, timeout) {
		// Can't get a ticket within timeout
		s.log.Warn("[Driver] Wait failed","err", ErrRateLimited,
			"msg_in", msg.Id())
		return nil, ErrRateLimited
	}
	now := time.Now()
	if !now.Before(end_allowed) {
		// Passed rate limit, but doesn't have time for Exchange().
		s.log.Warn("[Driver] Too late","err", ErrTimeout,
			"msg", msg.Id())
		return nil, ErrTimeout
	}
	// assert end_allowed.Sub(now) != 0
	msgs, err := s.Driver.Exchange(msg, end_allowed.Sub(now))
	if err != nil {
		s.log.Error("[Driver] Exchange err","err", err,
			"msg_in", msg.Id())
		return nil, err
	}
	s.log.Debug("[Driver] Exchange ok","msg_in", msg.Id(),
		"msg_out", msgs.Id())
	return msgs, nil
}

type RetryDriver struct {
	Driver
	Name string
	log log15.Logger
	backoffs []time.Duration
}

func NewRetryDriver(name string, driver Driver, backoffs[]time.Duration) *RetryDriver {
	var bk []time.Duration
	copy(bk, backoffs)
	return &RetryDriver{
		Driver:driver,
		Name:name,
		log:driver.Logger().New("mixin", name),
		backoffs:bk,
	}
}

func (s *RetryDriver) exchange(msg Message) (Messages, error) {
	bk := s.backoffs
	to_wait := 0 * time.Second
	count := 0
	for {
		count += 1
		s.log.Debug("[Driver] Exchange ..." , "count", count,
			"wait", to_wait, "msg_in", msg.Id())
		// A negative or zero duration causes Sleep to return immediately.
		time.Sleep(to_wait)
		// assert end_allowed.Sub(now) != 0
		msgs, err := s.Driver.Exchange(msg, 0)
		if err == nil {
			s.log.Debug("[Driver] Exchange ok","msg_in", msg.Id(),
				"msg_out", msgs.Id())
			return msgs, err
		}
		if len(bk) == 0 {
			// backoffs exhausted
			s.log.Error("[Driver] Exchange err, stop backing off",
				"err", err, "msg_in", msg.Id())
			return nil, err
		} else {
			s.log.Error("[Driver] Exchange err",
				"err", err, "msg_in", msg.Id())
		}
		to_wait = bk[0]
		bk = bk[1:]
	}
}

func (s *RetryDriver) Exchange(msg Message, timeout time.Duration) (Messages, error) {
	s.log.Debug("[Driver] Exchange()", "msg_in", msg.Id(),
		"timeout", timeout)
	if timeout == 0 {
		return s.exchange(msg)
	}
	end_allowed := time.Now().Add(timeout)
	tm := time.NewTimer(timeout)
	result := make(chan *tuple, 1)
	go func() {
		bk := s.backoffs
		to_wait := 0 * time.Second
		count := 0
		for {
			count += 1
			s.log.Debug("[Driver] Exchange ..." , "count", count,
				"wait", to_wait, "msg_in", msg.Id())
			now := time.Now()
			if !now.Add(to_wait).Before(end_allowed) {
				s.log.Error("[Driver] Exchange err, timeout" ,
					"msg_in", msg.Id())
				result <- &tuple{
					fst: nil,
					snd:ErrTimeout,
				}
				return
			}
			// A negative or zero duration causes Sleep to return immediately.
			time.Sleep(to_wait)
			// assert end_allowed.Sub(now) != 0
			msgs, err := s.Driver.Exchange(msg, end_allowed.Sub(now))
			if err == nil {
				s.log.Debug("[Driver] Exchange ok","msg_in", msg.Id(),
					"msg_out", msgs.Id())
				result <- &tuple{
					fst: msgs,
					snd:nil,
				}
				return
			}
			if len(bk) == 0 {
				// backoffs exhausted
				s.log.Error("[Driver] Exchange err, stop backing off",
					"err", err, "msg_in", msg.Id())
				result <- &tuple{
					fst: nil,
					snd:ErrTimeout,
				}
				return
			} else {
				s.log.Error("[Driver] Exchange err",
					"err", err, "msg_in", msg.Id())
			}
			to_wait = bk[0]
			bk = bk[1:]
		}
	}()
	select {
	case <-tm.C:
		return nil, ErrTimeout
	case tp := <-result:
		return tp.fst.(Messages), tp.snd.(error)
	}
}
