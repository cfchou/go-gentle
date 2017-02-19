// vim:fileencoding=utf-8
package service

import (
	"sync"
	"github.com/inconshreveable/log15"
	"github.com/afex/hystrix-go/hystrix"
	"time"
)

type MessageTuple struct {
	msg Message
	err error
}

// ChannelStream forms a stream from a channel.
// Functionally it's like DriverStream with ChannelDriver.
type ChannelStream struct {
	Name    string
	channel <-chan *MessageTuple
	log     log15.Logger
}

func NewChannelStream(name string, channel <-chan *MessageTuple) *ChannelStream {
	return &ChannelStream{
		Name:name,
		channel:channel,
		log:Log.New("mixin", "stream_chan", "name", name),
	}
}

func (r *ChannelStream) Logger() log15.Logger {
	return r.log
}

func (r *ChannelStream) Receive() (Message, error) {
	r.log.Debug("[Stream] Receive()")
	tp, ok := <- r.channel
	if !ok {
		return nil, ErrEOF
	}
	r.log.Debug("[Stream] Receive ok","msg_out", tp.msg.Id())
	return tp.msg, tp.err
}

// Turns a Driver to a Stream. It keeps executing
// driver.Exchange(genMessage()) to form a stream of Message's.
type DriverStream struct {
	Name         string
	driver       Driver
	log          log15.Logger
	msgs         chan interface{}
	genMessage   GenMessage
	once         sync.Once
}

func NewDriverStream(name string, driver Driver, max_queuing_messages int,
	genMessage GenMessage) *DriverStream {
	return &DriverStream{
		Name:         name,
		driver:       driver,
		log:Log.New("mixin", "stream_drv", "name", name),
		msgs:         make(chan interface{}, max_queuing_messages),
		genMessage: genMessage,
	}
}

func (r *DriverStream) onceDo() {
	go func() {
		r.log.Info("[Stream] once")
		for {
			// Viewed as an infinite source of events
			msgs, err := r.driver.Exchange(r.genMessage(), 0)
			if err != nil {
				r.msgs <- err
				continue
			}

			flattened_msgs := msgs.Flatten()
			for _, m := range flattened_msgs {
				r.msgs <- m
			}
		}
	}()
}

func (r *DriverStream) Receive() (Message, error) {
	r.log.Debug("[Stream] Receive()")
	r.once.Do(r.onceDo)
	switch m := (<-r.msgs).(type) {
	case error:
		return nil, m
	case Message:
		return m, nil
	default:
		panic("Never be here")
	}
}


// MapStream maps a Handler onto the upstream Stream. The results form
// a stream of Message's.
// It incorporates Bulkhead pattern using semaphore.
type MapStream struct {
	Stream
	Name string
	log       log15.Logger
	handler   Handler
	semaphore chan chan *tuple
	once sync.Once
}

func NewMapStream(name string, stream Stream, handler Handler,
	max_concurrent_handlers int) *MapStream {
	return &MapStream{
		Name:      name,
		Stream:  stream,
		log:Log.New("mixin", "stream_map", "name", name),
		handler:   handler,
		semaphore: make(chan chan *tuple, max_concurrent_handlers),
	}
}

func (r *MapStream) onceDo() {
	go func() {
		r.log.Info("[Stream] once")
		for {
			ret := make(chan *tuple, 1)
			msg, err := r.Stream.Receive()
			if err != nil {
				r.semaphore <- ret
				go func() {
					ret <- &tuple{
						fst: msg,
						snd: err,
					}
				}()
				continue
			}
			r.semaphore <- ret
			go func() {
				// TODO: Wrapf(e)?
				m, e := r.handler.Handle(msg)
				ret <- &tuple{
					fst: m,
					snd: e,
				}
			}()
		}
	}()
}

func (r *MapStream) Receive() (Message, error) {
	r.log.Debug("[Stream] Receive()")
	r.once.Do(r.onceDo)
	ret := <-<-r.semaphore
	return ret.fst.(Message), ret.snd.(error)
}

type RateLimitedStream struct {
	Stream
	Name string
	limiter RateLimit
	log       log15.Logger
}

func NewRateLimitedStream(name string, stream Stream, limiter RateLimit) *RateLimitedStream {
	return &RateLimitedStream{
		Stream:stream,
		Name: name,
		limiter:limiter,
		log:Log.New("mixin", "stream_rate", "name", name),
	}
}

func (r *RateLimitedStream) Receive() (Message, error) {
	r.log.Debug("[Stream] Receive()")
	r.limiter.Wait(1, 0)
	msg, err := r.Stream.Receive()
	if err != nil {
		r.log.Error("[Stream] Receive err","err", err)
		return nil, err
	}
	r.log.Debug("[Stream] Receive ok","msg_out", msg.Id())
	return msg, nil
}

// When Receive() encounters error, it backs off for some time
// and then retries.
type RetryStream struct {
	Stream
	Name string
	log       log15.Logger
	genBackoff GenBackOff
}

func NewRetryStream(name string, stream Stream, off GenBackOff) *RetryStream {
	return &RetryStream{
		Stream: stream,
		Name: name,
		genBackoff:off,
		log:Log.New("mixin", "stream_retry", "name", name),
	}
}

func (r *RetryStream) Receive() (Message, error) {
	r.log.Debug("[Stream] Receive()")
	var bk []time.Duration
	to_wait := 0 * time.Second
	count := 0
	for {
		count += 1
		r.log.Debug("[Stream] Receive ..." , "count", count,
			"wait", to_wait)
		// A negative or zero duration causes Sleep to return immediately.
		time.Sleep(to_wait)
		// assert end_allowed.Sub(now) != 0
		//
		msg, err := r.Stream.Receive()
		if err == nil {
			r.log.Debug("[Stream] Receive ok", "msg_out", msg.Id())
			return msg, err
		}
		if count == 1 {
			bk = r.genBackoff()
			r.log.Debug("[Stream] Receive generate backoffs",
				"len", len(bk))
		}
		if len(bk) == 0 {
			// backoffs exhausted
			r.log.Error("[Stream] Receive err, stop backing off",
				"err", err)
			return nil, err
		} else {
			r.log.Error("[Stream] Receive err", "err", err)
		}
		to_wait = bk[0]
		bk = bk[1:]
	}
}

type CircuitBreakerStream struct {
	Stream
	Name string
	log       log15.Logger
}

func NewCircuitBreakerStream(name string, stream Stream) *CircuitBreakerStream {
	return &CircuitBreakerStream{
		Stream:stream,
		Name:name,
		log:Log.New("mixin", "stream_circuit", "name", name),
	}
}

func (r *CircuitBreakerStream) Receive() (Message, error) {
	r.log.Debug("[Stream] Receive()")
	result := make(chan *tuple, 1)
	err := hystrix.Do(r.Name, func() error {
		msg, err := r.Stream.Receive()
		if err != nil {
			r.log.Error("[Stream] Receive err", "err", err)
			result <- &tuple{
				fst: msg,
				snd: err,
			}
			return err
		}
		r.log.Debug("[Stream] Receive ok", "msg_out", msg.Id())
		result <- &tuple{
			fst: msg,
			snd: err,
		}
		return nil
	}, nil)
	// hystrix.ErrTimeout doesn't interrupt work anyway.
	// It just contributes to circuit's metrics.
	if err != nil {
		r.log.Warn("[Stream] Circuit err","err", err)
		if err != hystrix.ErrTimeout {
			// Can be ErrCircuitOpen, ErrMaxConcurrency or
			// Receive()'s err.
			return nil, err
		}
	}
	tp := <-result
	return tp.fst.(Message), tp.snd.(error)
}


