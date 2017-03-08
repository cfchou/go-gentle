// vim:fileencoding=utf-8
package service

import (
	"sync"
	"github.com/inconshreveable/log15"
	"github.com/afex/hystrix-go/hystrix"
	"time"
)

// We have RateLimitedStream, RetryStream, CircuitBreakerStream, BulkheadStream.
// Each of which, when mixes(encompasses) each other, brings different
// resiliency options.
// A Stream applies its resiliency pattern to its upstream(enclosed Stream).
// Itself could also be another Stream's upstream. The decision of what Streams
// and in which order to mix must be sane.

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
	return nil, tp.snd.(error)
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
	defer func(){<- r.semaphore}()
	msg, err := r.Stream.Receive()
	if err == nil {
		r.log.Debug("[Stream] Receive ok", "msg_out", msg.Id())
	} else {
		r.log.Error("[Stream] Receive err", "err", err)
	}
	return msg, err
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

func (r *ChannelStream) Logger() log15.Logger {
	return r.log
}

func (r *ChannelStream) Receive() (Message, error) {
	r.log.Debug("[Stream] Receive()")
	msg := <-r.channel
	r.log.Debug("[Stream] Receive ok", "msg_out", msg.Id())
	return msg, nil
}

// ConcurrentFetchStream concurrently prefetch a number of items from upstream
// without being asked.
// Note that the order of messages emitted from the upstream may not be
// preserved. It's down to application to maintain the order if that's required.
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
		receives: make(chan *tuple, max_concurrency),
		semaphore: make(chan *struct{}, max_concurrency),
	}
}

func (r *ConcurrentFetchStream) onceDo() {
	go func() {
		r.log.Info("[Stream] once")
		for {
			// pull more messages as long as semaphore allows
			r.semaphore <- &struct{}{}
			// Since Receive() are run concurrently, the order of
			// elements from upstream may not preserved.
			go func() {
				r.log.Debug("[Stream] onceDo Receive()")
				msg, err := r.Stream.Receive()
				if err == nil {
					r.log.Debug("[Stream] onceDo Receive ok",
						"msg_out", msg.Id())
				} else {
					r.log.Error("[Stream] onceDo Receive err",
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

func (r *ConcurrentFetchStream) Receive() (Message, error) {
	r.log.Debug("[Stream] Receive()")
	r.once.Do(r.onceDo)
	tp := <-r.receives
	<-r.semaphore
	if tp.snd != nil {
		err := tp.snd.(error)
		r.log.Error("[Stream] Receive err", "err", err)
		return nil, err
	}
	msg := tp.fst.(Message)
	r.log.Debug("[Stream] Receive ok", "msg_out", msg.Id())
	return msg, nil
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
		log:       Log.New("mixin", "mappedStream", "name", name),
		handler:   handler,
	}
}

func (r *MappedStream) Receive() (Message, error) {
	r.log.Debug("[Stream] Receive()")
	msg, err := r.Stream.Receive()
	if err != nil {
		r.log.Error("[Stream] Receive err", "err", err)
		return nil, err
	}
	r.log.Debug("[Stream] Receive ok, run Handle()", "msg", msg.Id())
	m, e := r.handler.Handle(msg)
	if e != nil {
		r.log.Error("[Stream] Handle err", "err", err)
		return nil, e
	}
	r.log.Debug("[Stream] Handle done", "msg_in", msg.Id(),
		"msg_out", m.Id())
	return m, nil
}
