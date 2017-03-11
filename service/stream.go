package service

import (
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/inconshreveable/log15"
	"sync"
	"time"
)

// We have RateLimitedStream, RetryStream, CircuitBreakerStream, BulkheadStream.
// Each of which, when mixes(encompasses) each other, brings different
// resiliency options.
// A Stream applies its resiliency pattern to its upstream(enclosed Stream).
// Itself could also be another Stream's upstream. The decision of what Streams
// and in which order to mix must be sane.

type RateLimitedStream struct {
	Name    string
	stream  Stream
	limiter RateLimit
	log     log15.Logger
}

func NewRateLimitedStream(name string, stream Stream, limiter RateLimit) *RateLimitedStream {
	return &RateLimitedStream{
		stream:  stream,
		Name:    name,
		limiter: limiter,
		log:     Log.New("mixin", "stream_rate", "name", name),
	}
}

func (r *RateLimitedStream) Get() (Message, error) {
	r.log.Debug("[Stream] Get()")
	r.limiter.Wait(1, 0)
	msg, err := r.stream.Get()
	if err != nil {
		r.log.Error("[Stream] Get err", "err", err)
		return nil, err
	}
	r.log.Debug("[Stream] Get ok", "msg_out", msg.Id())
	return msg, nil
}

// When Get() encounters error, it backs off for some time
// and then retries.
type RetryStream struct {
	Name       string
	stream     Stream
	log        log15.Logger
	genBackoff GenBackOff
}

func NewRetryStream(name string, stream Stream, off GenBackOff) *RetryStream {
	return &RetryStream{
		stream:     stream,
		Name:       name,
		genBackoff: off,
		log:        Log.New("mixin", "stream_retry", "name", name),
	}
}

func (r *RetryStream) Get() (Message, error) {
	r.log.Debug("[Stream] Get()")
	var bk []time.Duration
	to_wait := 0 * time.Second
	count := 0
	for {
		count += 1
		r.log.Debug("[Stream] Get ...", "count", count,
			"wait", to_wait)
		// A negative or zero duration causes Sleep to return immediately.
		time.Sleep(to_wait)
		// assert end_allowed.Sub(now) != 0
		msg, err := r.stream.Get()
		if err == nil {
			r.log.Debug("[Stream] Get ok", "msg_out", msg.Id())
			return msg, err
		}
		if count == 1 {
			bk = r.genBackoff()
			r.log.Debug("[Stream] Get generate backoffs",
				"len", len(bk))
		}
		if len(bk) == 0 {
			// backoffs exhausted
			r.log.Error("[Stream] Get err, stop backing off",
				"err", err)
			return nil, err
		} else {
			r.log.Error("[Stream] Get err", "err", err)
		}
		to_wait = bk[0]
		bk = bk[1:]
	}
}

// Bulkhead pattern is used to limit the number of concurrent Get().
// Calling Get() is blocked when exceeding the limit.
type BulkheadStream struct {
	Name      string
	stream    Stream
	log       log15.Logger
	semaphore chan *struct{}
}

func NewBulkheadStream(name string, stream Stream, max_concurrency int) *BulkheadStream {
	if max_concurrency <= 0 {
		panic(errors.New("max_concurrent must be greater than 0"))
	}
	return &BulkheadStream{
		Name:      name,
		stream:    stream,
		log:       Log.New("mixin", "stream_bulk", "name", name),
		semaphore: make(chan *struct{}, max_concurrency),
	}
}

func (r *BulkheadStream) Get() (Message, error) {
	r.log.Debug("[Stream] Get()")
	r.semaphore <- &struct{}{}
	defer func() { <-r.semaphore }()
	msg, err := r.stream.Get()
	if err == nil {
		r.log.Debug("[Stream] Get ok", "msg_out", msg.Id())
	} else {
		r.log.Error("[Stream] Get err", "err", err)
	}
	return msg, err
}

// CircuitBreaker pattern using hystrix-go.
type CircuitBreakerStream struct {
	Name    string
	Circuit string
	stream  Stream
	log     log15.Logger
}

func NewCircuitBreakerStream(name string, stream Stream, circuit string) *CircuitBreakerStream {
	return &CircuitBreakerStream{
		Name:    name,
		Circuit: circuit,
		stream:  stream,
		log: Log.New("mixin", "stream_circuit", "name", name,
			"circuit", circuit),
	}
}

func (r *CircuitBreakerStream) Get() (Message, error) {
	r.log.Debug("[Stream] Get()")
	result := make(chan *tuple, 1)
	err := hystrix.Do(r.Circuit, func() error {
		msg, err := r.stream.Get()
		if err != nil {
			r.log.Error("[Stream] Get err", "err", err)
			result <- &tuple{
				fst: msg,
				snd: err,
			}
			return err
		}
		r.log.Debug("[Stream] Get ok", "msg_out", msg.Id())
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
			// Get()'s err.
			return nil, err
		}
	}
	tp := <-result
	if tp.snd == nil {
		return tp.fst.(Message), nil
	}
	return nil, tp.snd.(error)
}

// ChannelStream forms a stream from a channel.
type ChannelStream struct {
	Name    string
	channel <-chan Message
	log     log15.Logger
}

func NewChannelStream(name string, channel <-chan Message) *ChannelStream {
	return &ChannelStream{
		Name:    name,
		channel: channel,
		log:     Log.New("mixin", "chanStream", "name", name),
	}
}

func (r *ChannelStream) Get() (Message, error) {
	r.log.Debug("[Stream] Get()")
	msg := <-r.channel
	r.log.Debug("[Stream] Get ok", "msg_out", msg.Id())
	return msg, nil
}

// ConcurrentFetchStream concurrently prefetch a number of items from upstream
// without being asked.
// Note that the order of messages emitted from the upstream may not be
// preserved. It's down to application to maintain the order if that's required.
// For example, set a sequence number inside Messages.
type ConcurrentFetchStream struct {
	Name      string
	stream    Stream
	log       log15.Logger
	receives  chan *tuple
	semaphore chan *struct{}
	once      sync.Once
}

func NewConcurrentFetchStream(name string, stream Stream, max_concurrency int) *ConcurrentFetchStream {
	return &ConcurrentFetchStream{
		Name:      name,
		stream:    stream,
		log:       Log.New("mixin", "fetchStream", "name", name),
		receives:  make(chan *tuple, max_concurrency),
		semaphore: make(chan *struct{}, max_concurrency),
	}
}

func (r *ConcurrentFetchStream) onceDo() {
	go func() {
		r.log.Info("[Stream] once")
		for {
			// pull more messages as long as semaphore allows
			r.semaphore <- &struct{}{}
			// Since Get() are run concurrently, the order of
			// elements from upstream may not preserved.
			go func() {
				r.log.Debug("[Stream] onceDo Get()")
				msg, err := r.stream.Get()
				if err == nil {
					r.log.Debug("[Stream] onceDo Get ok",
						"msg_out", msg.Id())
				} else {
					r.log.Error("[Stream] onceDo Get err",
						"err", err)
				}
				r.receives <- &tuple{
					fst: msg,
					snd: err,
				}
			}()
		}
	}()
}

func (r *ConcurrentFetchStream) Get() (Message, error) {
	r.log.Debug("[Stream] Get()")
	r.once.Do(r.onceDo)
	tp := <-r.receives
	<-r.semaphore
	if tp.snd != nil {
		err := tp.snd.(error)
		r.log.Error("[Stream] Get err", "err", err)
		return nil, err
	}
	msg := tp.fst.(Message)
	r.log.Debug("[Stream] Get ok", "msg_out", msg.Id())
	return msg, nil
}

// MappedStream maps a Handler onto the upstream Stream. The results form
// a stream of Message's.
type MappedStream struct {
	Name    string
	stream  Stream
	log     log15.Logger
	handler Handler
	once    sync.Once
}

func NewMappedStream(name string, stream Stream, handler Handler) *MappedStream {
	return &MappedStream{
		Name:    name,
		stream:  stream,
		log:     Log.New("mixin", "mappedStream", "name", name),
		handler: handler,
	}
}

func (r *MappedStream) Get() (Message, error) {
	r.log.Debug("[Stream] Get()")
	msg, err := r.stream.Get()
	if err != nil {
		r.log.Error("[Stream] Get err", "err", err)
		return nil, err
	}
	r.log.Debug("[Stream] Get ok, run Handle()", "msg", msg.Id())
	m, e := r.handler.Handle(msg)
	if e != nil {
		r.log.Error("[Stream] Handle err", "err", err)
		return nil, e
	}
	r.log.Debug("[Stream] Handle done", "msg_in", msg.Id(),
		"msg_out", m.Id())
	return m, nil
}
