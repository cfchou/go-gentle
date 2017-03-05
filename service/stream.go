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
		Name:    name,
		channel: channel,
		log:     Log.New("mixin", "chanStream", "name", name),
	}
}

func (r *ChannelStream) Logger() log15.Logger {
	return r.log
}

func (r *ChannelStream) Receive() (Message, error) {
	r.log.Debug("[Stream] Receive()")
	tp, ok := <-r.channel
	if !ok {
		r.log.Debug("[Stream] Receive EOF")
		return nil, ErrEOF
	}
	r.log.Debug("[Stream] Receive ok", "msg_out", tp.msg.Id())
	return tp.msg, tp.err
}

// Turns a Driver to a Stream. It keeps calling
// driver.Exchange(inputStream.Receive()) to form a stream of Message's.
type DriverStream struct {
	Name        string
	driver      Driver
	log         log15.Logger
	msgs        chan interface{}
	inputStream Stream
	once        sync.Once
}

func NewDriverStream(name string, driver Driver, max_queuing_messages int,
	genInputMessage Stream) *DriverStream {
	return &DriverStream{
		Name:        name,
		driver:      driver,
		log:         Log.New("mixin", "driverStream", "name", name),
		msgs:        make(chan interface{}, max_queuing_messages),
		inputStream: genInputMessage,
	}
}

func (r *DriverStream) onceDo() {
	go func() {
		r.log.Info("[Stream] once")
		for {
			// Viewed as an infinite source of events
			msg, err := r.inputStream.Receive()
			if err != nil {
				r.log.Error("[Stream] Receive failed",
					"err", err)
				r.msgs <- err
				continue
			}
			r.log.Debug("[Stream] Receive ok",
				"msg_out", msg.Id())
			meta_messages, err := r.driver.Exchange(msg, 0)
			if err != nil {
				r.log.Error("[Stream] Exchange err", "err", err)
				r.msgs <- err
				continue
			}
			r.log.Debug("[Stream] Exchange ok",
				"msg_out", meta_messages.Id())

			flattened_msgs := meta_messages.Flatten()
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
		r.log.Debug("[Stream] Receive ok", "msg_out", m.Id())
		return m, nil
	default:
		panic("Never be here")
	}
}

type RateLimitedStream struct {
	Stream
	Name    string
	limiter RateLimit
	log     log15.Logger
}

func NewRateLimitedStream(name string, stream Stream, limiter RateLimit) *RateLimitedStream {
	return &RateLimitedStream{
		Stream:  stream,
		Name:    name,
		limiter: limiter,
		log:     Log.New("mixin", "stream_rate", "name", name),
	}
}

func (r *RateLimitedStream) Receive() (Message, error) {
	r.log.Debug("[Stream] Receive()")
	r.limiter.Wait(1, 0)
	msg, err := r.Stream.Receive()
	if err != nil {
		r.log.Error("[Stream] Receive err", "err", err)
		return nil, err
	}
	r.log.Debug("[Stream] Receive ok", "msg_out", msg.Id())
	return msg, nil
}

// When Receive() encounters error, it backs off for some time
// and then retries.
type RetryStream struct {
	Stream
	Name       string
	log        log15.Logger
	genBackoff GenBackOff
}

func NewRetryStream(name string, stream Stream, off GenBackOff) *RetryStream {
	return &RetryStream{
		Stream:     stream,
		Name:       name,
		genBackoff: off,
		log:        Log.New("mixin", "stream_retry", "name", name),
	}
}

func (r *RetryStream) Receive() (Message, error) {
	r.log.Debug("[Stream] Receive()")
	var bk []time.Duration
	to_wait := 0 * time.Second
	count := 0
	for {
		count += 1
		r.log.Debug("[Stream] Receive ...", "count", count,
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

// CircuitBreaker pattern using hystrix-go.
type CircuitBreakerStream struct {
	Stream
	Name string
	log  log15.Logger
}

func NewCircuitBreakerStream(name string, stream Stream) *CircuitBreakerStream {
	return &CircuitBreakerStream{
		Stream: stream,
		Name:   name,
		log:    Log.New("mixin", "stream_circuit", "name", name),
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
		r.log.Warn("[Stream] Circuit err", "err", err)
		if err != hystrix.ErrTimeout {
			// Can be ErrCircuitOpen, ErrMaxConcurrency or
			// Receive()'s err.
			return nil, err
		}
	}
	tp := <-result
	if tp.snd == nil {
		return tp.fst.(Message), nil
	}
	return tp.fst.(Message), tp.snd.(error)
}

// Bulkhead pattern is used to limit the number of concurrent Receive().
// Calling Receive() is blocked when exceeding the limit.
type BulkheadStream struct {
	Stream
	Name      string
	log       log15.Logger
	semaphore chan *struct{}
	once      sync.Once
}

func NewBulkheadStream(name string, stream Stream, max_concurrency int) *BulkheadStream {
	return &BulkheadStream{
		Name:      name,
		Stream:    stream,
		log:       Log.New("mixin", "stream_bulk", "name", name),
		semaphore: make(chan *struct{}, max_concurrency),
	}
}

func (r *BulkheadStream) Receive() (Message, error) {
	r.log.Debug("[Stream] Receive()")
	r.semaphore <- &struct{}{}
	msg, err := r.Stream.Receive()
	if err == nil {
		r.log.Debug("[Stream] Receive ok", "msg_out", msg.Id())
	} else {
		r.log.Error("[Stream] Receive err", "err", err)
	}
	<- r.semaphore
	return msg, err
}

// ConcurrentFetchStream concurrently prefetch a number of items from upstream
// without being asked.
type ConcurrentFetchStream struct {
	Stream
	Name      string
	log       log15.Logger
	receives 	  chan *tuple
	semaphore chan *struct{}
	once      sync.Once
}

func NewConcurrentFetchStream(name string, stream Stream, max_concurrency int) *ConcurrentFetchStream {
	return &ConcurrentFetchStream{
		Name:      name,
		Stream:    stream,
		log:       Log.New("mixin", "fetchStream", "name", name),
		receives: make(chan *tuple, max_concurrency + 1),
		semaphore: make(chan *struct{}, max_concurrency),
	}
}

func (r *ConcurrentFetchStream) onceDo() {
	go func() {
		r.log.Info("[Stream] once")
		for {
			// pull more messages as long as semaphore allows
			r.semaphore <- &struct{}{}
			go func() {
				msg, err := r.Stream.Receive()
				if err == nil {
					r.log.Debug("[Stream] Receive ok",
						"msg_out", msg.Id())
				} else {
					r.log.Error("[Stream] Receive err",
						"err", err)
				}
				// cap(receives) is max_concurrency + 1, so that
				// it wouldn't yield here and therefore the
				// order of received messages is preserved.
				r.receives <- &tuple{
					fst: msg,
					snd: err,
				}
			}()
		}
	}()
}

func (r *ConcurrentFetchStream) Receive() (Message, error) {
	r.log.Debug("[Stream] Receive()")
	r.once.Do(r.onceDo)
	recv := <-r.receives
	<-r.semaphore
	if recv.snd == nil {
		return recv.fst.(Message), nil
	}
	return recv.fst.(Message), recv.snd.(error)
}

// MappedStream maps a Handler onto the upstream Stream. The results form
// a stream of Message's.
type MappedStream struct {
	Stream
	Name      string
	log       log15.Logger
	handler   Handler
	once      sync.Once
}

func NewMappedStream(name string, stream Stream, handler Handler) *MappedStream {
	return &MappedStream{
		Name:      name,
		Stream:    stream,
		log:       Log.New("mixin", "handlerStream", "name", name),
		handler:   handler,
	}
}

func (r *MappedStream) Receive() (Message, error) {
	msg, err := r.Stream.Receive()
	if err != nil {
		return r.handler.Handle(msg)
	}
	return nil, err
}
