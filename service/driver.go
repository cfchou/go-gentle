// vim:fileencoding=utf-8
package service

import (
	"github.com/inconshreveable/log15"
	"time"
	"sync"
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
		log: channel.Logger().New("mixin", "rate"),
	}
}

func (s *RateLimitedDriver) Exchange(msg Message, timeout time.Duration) (Messages, error) {
	s.log.Debug("[Driver] Exchange...", "msg", msg.Id())
	if timeout == 0 {
		s.limiter.Wait(1, 0)
		s.log.Debug("[Driver] Exchange ok","msg", msg.Id())
		return s.Driver.Exchange(msg, timeout)
	}

	begin := time.Now()
	end_allowed := begin.Add(timeout)
	if !s.limiter.Wait(1, timeout) {
		s.log.Warn("[Driver] Wait failed","err", ErrRateLimited,
			"msg", msg.Id())
		return nil, ErrRateLimited
	}
	end := time.Now()
	if !end.Before(end_allowed) {
		// Passed rate limiter, but doesn't have time for Exchange().
		s.log.Warn("[Driver] Too late","err", ErrTimeout,
			"msg", msg.Id())
		return nil, ErrTimeout
	}
	s.log.Debug("[Driver] Exchange ok", "msg", msg.Id())
	return s.Driver.Exchange(msg, end_allowed.Sub(end))
}


// Turns a Driver to a Receiver
type DriverReceiver struct {
	Name string
	driver Driver
	log log15.Logger
	msgs chan interface{}
	fixedRequest Message
	once sync.Once
}

func NewDriverReceiver(name string, driver Driver, max_queuing_messages int,
	fixed_request Message) *DriverReceiver {
	return &DriverReceiver{
		Name: name,
		driver:driver,
		log:driver.Logger().New("mixin", name),
		msgs: make(chan interface{}, max_queuing_messages),
		fixedRequest:fixed_request,
	}
}

func (r *DriverReceiver) onceDo() {
	r.log.Info("[Receiver] once")
	for {
		// Viewed as an infinite source of events
		msgs, err := r.driver.Exchange(r.fixedRequest, 0)
		if err != nil {
			r.msgs <- err
			continue
		}

		flattened_msgs := msgs.Flatten()
		for _, m := range flattened_msgs {
			r.msgs <- m
		}
	}
}

func (r *DriverReceiver) Receive() (Message, error) {
	r.once.Do(r.onceDo)
	switch m := (<- r.msgs).(type) {
	case error:
		return nil, m
	case Message:
		return m, nil
	default:
		panic("Never be here")
	}
}

// It should be used in an asynchronous fashion to maximize throughput.
type HandlerReactor struct {
	Name string
	Receiver

	log log15.Logger
	handler Handler
	semaphore chan *struct{}
	once sync.Once
}

func NewHandlerReactor(name string, receiver Receiver, handler Handler,
	max_concurrent_handlers int) *HandlerReactor {
	return &HandlerReactor{
		Name:name,
		Receiver:receiver,
		log: receiver.Logger().New("mixin", name),
		handler:handler,
		semaphore:make(chan *struct{}, max_concurrent_handlers),
	}
}

func (r *HandlerReactor) Receive() (Message, error) {
	msg, err := r.Receiver.Receive()
	if err != nil {
		return nil, err
	}
	r.semaphore <- &struct{}{}
	defer func() {
		<- r.semaphore
	}()
	return r.handler(msg)
}

/*
type BackOffReceiver struct {
	Name string
	Receiver

	log log15.Logger

}
*/


// Note that BackOffDriver is thread-safe by serializing ReceiveMessage().
// Safety is favoured over performance.
type BackOffDriver struct {
	Name string
	Driver

	log log15.Logger
	lock sync.Mutex
	backoffs []time.Duration
	next time.Time
	last time.Time
}

// TODO mixed back-off: exponential back-off followed by constant back-off
func NewBackOffDriver(name string, driver Driver) *BackOffDriver {
	return &BackOffDriver{
		Name: name,
		Driver: driver,
		log: driver.Logger().New("mixin", "backoff"),
		next: time.Now(),
		last: time.Now(),
	}
}

func (s *BackOffDriver) GenerateBackOffs() []time.Duration {
	return []time.Duration{1*time.Second, 2*time.Second}
}

func (s *BackOffDriver) nextBackOff() time.Duration {
	if !s.isBackingOff() {
		panic("Not in backoff state")
	}
	return s.backoffs[0]
}

func (s *BackOffDriver) newBackOffs() {
	s.backoffs = s.GenerateBackOffs()
	if len(s.backoffs) == 0 {
		panic("GenerateBackOffs() length is 0")
	}
}

func (s *BackOffDriver) enableBackOff() {
	s.newBackOffs()
}

func (s *BackOffDriver) disableBackOff() {
	s.backoffs = []time.Duration{}
}

func (s *BackOffDriver) isBackingOff() bool {
	return len(s.backoffs) > 0
}

func (s *BackOffDriver) advanceBackOff() {
	if !s.isBackingOff() {
		panic("Not in backoff state")
	}
	if len(s.backoffs) == 1 {
		s.newBackOffs()
	} else {
		s.backoffs = s.backoffs[1:]
	}
}

func (s *BackOffDriver) exchange(msg Message) (Messages, error) {
	for {
		s.lock.Lock()
		if !s.isBackingOff() {
			s.lock.Unlock()
			break
		}
		// isBackingOff
		now := time.Now()
		if !now.Before(s.next) {
			if s.isBackingOff() {
				s.next = s.next.Add(s.nextBackOff())
				s.advanceBackOff()
			}
			s.lock.Unlock()
			break
		}
		// now before s.next
		wait := s.next.Sub(now)
		s.lock.Unlock()
		time.Sleep(wait)
	}
	// given the green light
	msgs, err := s.Driver.Exchange(msg, 0)
	end := time.Now()
	s.lock.Lock()
	if err != nil && !s.isBackingOff() && end.After(s.last) {
		// s.last < end
		s.newBackOffs()
		s.next = s.next.Add(s.nextBackOff())
		s.advanceBackOff()
	} else if err == nil && s.isBackingOff() && !end.Before(s.last){
		// s.last <= end
		s.disableBackOff()
		s.next = time.Now()
	}
	s.lock.Unlock()
	return msgs, err
}

func (s *BackOffDriver) Exchange(msg Message, timeout time.Duration) (Messages, error) {
	if timeout == 0 {
		return s.exchange(msg)
	}

	finish_at := time.Now().Add(timeout)
	for {
		s.lock.Lock()
		if !s.isBackingOff() {
			s.lock.Unlock()
			break
		}

		// isBackingOff
		now := time.Now()
		if !now.Before(finish_at) || !s.next.Before(finish_at) {
			s.lock.Unlock()
			return nil, ErrTimeout
		}
		if !now.Before(s.next) {
			if s.isBackingOff() {
				s.next = s.next.Add(s.nextBackOff())
				s.advanceBackOff()
			}
			s.lock.Unlock()
			break
		}
		wait := s.next.Sub(now)
		s.lock.Unlock()
		time.Sleep(wait)
	}
	begin := time.Now()
	if !begin.Before(finish_at) {
		return nil, ErrTimeout
	}
	// given the green light
	msgs, err := s.Driver.Exchange(msg, finish_at.Sub(begin))
	end := time.Now()
	go func() {
		s.lock.Lock()
		defer s.lock.Unlock()
		if err != nil && !s.isBackingOff() && end.After(s.last) {
			// s.last < end
			s.newBackOffs()
			s.next = s.next.Add(s.nextBackOff())
			s.advanceBackOff()
		} else if err == nil && s.isBackingOff() && !end.Before(s.last){
			// s.last <= end
			s.disableBackOff()
			s.next = time.Now()
		}
		s.lock.Unlock()
	}()
	return msgs, err
}
