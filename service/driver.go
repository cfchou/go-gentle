// vim:fileencoding=utf-8
package service

import (
	"github.com/inconshreveable/log15"
	"time"
	"github.com/afex/hystrix-go/hystrix"
)

type MetaMessageTuple struct {
	metaMessage MetaMessage
	err         error
}

// ChannelDriver pull values from a channel.
type ChannelDriver struct {
	Name    string
	channel <-chan *MetaMessageTuple
	log     log15.Logger
}

func NewChannelDriver(name string, channel <-chan *MetaMessageTuple) *ChannelDriver {
	return &ChannelDriver{
		Name:    name,
		channel: channel,
		log:     Log.New("mixin", "drv_chan", "name", name),
	}
}

func (s *ChannelDriver) Exchange(msg Message, timeout time.Duration) (MetaMessage, error) {
	// msg is not used, but it's still here for logging.
	s.log.Debug("[Driver] Exchange()", "msg_in", msg.Id())
	if timeout == 0 {
		tp, ok := <-s.channel
		if !ok {
			s.log.Debug("[Driver] Exchange EOF", "msg_in", msg.Id())
			return nil, ErrEOF
		}
		s.log.Debug("[Driver] Exchange ok", "msg_in", msg.Id(),
			"msg_out", tp.metaMessage.Id())
		return tp.metaMessage, tp.err
	}
	tm := time.NewTimer(timeout)
	select {
	case <-tm.C:
		s.log.Error("[Driver] Exchange timeout", "msg_in", msg.Id())
		return nil, ErrTimeout
	case tp := <-s.channel:
		s.log.Debug("[Driver] Exchange ok", "msg_in", msg.Id(),
			"msg_out", tp.metaMessage.Id())
		return tp.metaMessage, tp.err
	}
}

func (s *ChannelDriver) Logger() log15.Logger {
	return s.log
}

type RateLimitedDriver struct {
	Driver
	Name    string
	limiter RateLimit
	log     log15.Logger
}

func NewRateLimitedDriver(name string, driver Driver, limiter RateLimit) *RateLimitedDriver {
	return &RateLimitedDriver{
		Driver:  driver,
		Name:    name,
		limiter: limiter,
		log:     Log.New("mixin", "drv_rate", "name", name),
	}
}

func (s *RateLimitedDriver) exchange(msg Message) (MetaMessage, error) {
	s.limiter.Wait(1, 0)
	metaMessage, err := s.Driver.Exchange(msg, 0)
	if err != nil {
		s.log.Error("[Driver] Exchange err", "err", err,
			"msg_in", msg.Id())
		return nil, err
	}
	s.log.Debug("[Driver] Exchange ok", "msg_in", msg.Id(),
		"msg_out", metaMessage.Id())
	return metaMessage, nil
}

func (s *RateLimitedDriver) Exchange(msg Message, timeout time.Duration) (MetaMessage, error) {
	s.log.Debug("[Driver] Exchange()", "msg_in", msg.Id(),
		"timeout", timeout)
	if timeout == 0 {
		return s.exchange(msg)
	}
	end_allowed := time.Now().Add(timeout)
	if !s.limiter.Wait(1, timeout) {
		// Can't get a ticket within timeout
		s.log.Warn("[Driver] Wait failed", "err", ErrRateLimited,
			"msg_in", msg.Id())
		return nil, ErrRateLimited
	}
	now := time.Now()
	if !now.Before(end_allowed) {
		// Passed rate limit, but doesn't have time for Exchange().
		s.log.Warn("[Driver] Too late", "err", ErrTimeout,
			"msg", msg.Id())
		return nil, ErrTimeout
	}
	// assert end_allowed.Sub(now) != 0
	metaMessage, err := s.Driver.Exchange(msg, end_allowed.Sub(now))
	if err != nil {
		s.log.Error("[Driver] Exchange err", "err", err,
			"msg_in", msg.Id())
		return nil, err
	}
	s.log.Debug("[Driver] Exchange ok", "msg_in", msg.Id(),
		"msg_out", metaMessage.Id())
	return metaMessage, nil
}

// When Exchange(msg, timeout) encounters error, it backs off for some time
// and then retries.
// Note that the times used by back-offs and failed retries do not consume
// timeout so the same timeout is given to every retry.
type RetryDriver struct {
	Driver
	Name       string
	log        log15.Logger
	genBackoff GenBackOff
}

func NewRetryDriver(name string, driver Driver, off GenBackOff) *RetryDriver {
	return &RetryDriver{
		Driver:     driver,
		Name:       name,
		genBackoff: off,
		log:        Log.New("mixin", "drv_retry", "name", name),
	}
}

func (s *RetryDriver) Exchange(msg Message, timeout time.Duration) (MetaMessage, error) {
	s.log.Debug("[Driver] Exchange()", "msg_in", msg.Id())
	var bk []time.Duration
	to_wait := 0 * time.Second
	count := 0
	for {
		count += 1
		s.log.Debug("[Driver] Exchange ...", "count", count,
			"wait", to_wait, "msg_in", msg.Id())
		// A negative or zero duration causes Sleep to return immediately.
		time.Sleep(to_wait)
		// assert end_allowed.Sub(now) != 0
		//
		metaMessage, err := s.Driver.Exchange(msg, timeout)
		if err == nil {
			s.log.Debug("[Driver] Exchange ok", "msg_in", msg.Id(),
				"msg_out", metaMessage.Id())
			return metaMessage, err
		}
		if count == 1 {
			bk = s.genBackoff()
			s.log.Debug("[Driver] Exchange generate backoffs",
				"len", len(bk), "msg_in", msg.Id())
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

type CircuitBreakerDriver struct {
	Driver
	Name string
	log  log15.Logger
}

func NewCircuitBreakerDriver(name string, driver Driver) *CircuitBreakerDriver {
	return &CircuitBreakerDriver{
		Driver: driver,
		Name:   name,
		log:    Log.New("mixin", "drv_circuit", "name", name),
	}
}

func (s *CircuitBreakerDriver) Exchange(msg Message, timeout time.Duration) (MetaMessage, error) {
	s.log.Debug("[Driver] Exchange()", "msg_in", msg.Id(),
		"timeout", timeout)
	result := make(chan *tuple, 1)
	err := hystrix.Do(s.Name, func() error {
		metaMessage, err := s.Driver.Exchange(msg, timeout)
		if err != nil {
			s.log.Error("[Driver] Exchange err", "err", err,
				"msg_in", msg.Id())
			result <- &tuple{
				fst: metaMessage,
				snd: err,
			}
			return err
		}
		s.log.Debug("[Driver] Exchange ok", "msg_in", msg.Id(),
			"msg_out", metaMessage.Id())
		result <- &tuple{
			fst: msg,
			snd: err,
		}
		return nil
	}, nil)
	// hystrix.Do() is synchronous so at this point there are three
	// possibilities:
	// 1. work function is prevented from execution, err contains
	//    hystrix.ErrCircuitOpen or hystrix.ErrMaxConcurrency.
	// 2. work function is finished before hystrix's timeout. err is nil or
	//    an error returned by work.
	// 3. work function is finished after hystrix's timeout. err is
	//    hystrix.ErrTimeout or an error returned by work.
	// In case 2 and 3 we'd like to return Exchange()'s result even it's
	// error.
	// hystrix.ErrTimeout doesn't interrupt work anyway.
	// It just contributes to circuit's metrics.
	if err != nil {
		s.log.Warn("[Driver] Circuit err", "err", err, "msg_in", msg.Id())
		if err != hystrix.ErrTimeout {
			// Can be ErrCircuitOpen, ErrMaxConcurrency or
			// Exchange()'s err.
			return nil, err
		}
	}
	tp := <-result
	return tp.fst.(MetaMessage), tp.snd.(error)
}
