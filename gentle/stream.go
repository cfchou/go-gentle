package gentle

import (
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	log15 "gopkg.in/inconshreveable/log15.v2"
	"sync"
	"time"
)

// Rate limiting pattern is used to limit the speed of a series of Get().
type RateLimitedStream struct {
	Name    string
	Log     log15.Logger
	stream  Stream
	limiter RateLimit
}

func NewRateLimitedStream(name string, stream Stream, limiter RateLimit) *RateLimitedStream {
	return &RateLimitedStream{
		Name:    name,
		Log:     Log.New("mixin", "stream_rate", "name", name),
		stream:  stream,
		limiter: limiter,
	}
}

// Get() is blocked when the limit is exceeded.
func (r *RateLimitedStream) Get() (Message, error) {
	r.Log.Debug("[Stream] Get()")
	r.limiter.Wait(1, 0)
	msg, err := r.stream.Get()
	if err != nil {
		r.Log.Error("[Stream] Get err", "err", err)
		return nil, err
	}
	r.Log.Debug("[Stream] Get ok", "msg_out", msg.Id())
	return msg, nil
}

// RetryStream will, when Get() encounters error, back off for some time
// and then retries.
type RetryStream struct {
	Name       string
	Log        log15.Logger
	stream     Stream
	genBackoff GenBackOff
}

func NewRetryStream(name string, stream Stream, off GenBackOff) *RetryStream {
	return &RetryStream{
		Name:       name,
		Log:        Log.New("mixin", "stream_retry", "name", name),
		stream:     stream,
		genBackoff: off,
	}
}

func (r *RetryStream) Get() (Message, error) {
	r.Log.Debug("[Stream] Get()")
	var bk []time.Duration
	to_wait := 0 * time.Second
	count := 0
	for {
		count += 1
		r.Log.Debug("[Stream] Get ...", "count", count,
			"wait", to_wait)
		// A negative or zero duration causes Sleep to return immediately.
		time.Sleep(to_wait)
		// assert end_allowed.Sub(now) != 0
		msg, err := r.stream.Get()
		if err == nil {
			r.Log.Debug("[Stream] Get ok", "msg_out", msg.Id())
			return msg, err
		}
		if count == 1 {
			bk = r.genBackoff()
			r.Log.Debug("[Stream] Get generate backoffs",
				"len", len(bk))
		}
		if len(bk) == 0 {
			// backoffs exhausted
			r.Log.Error("[Stream] Get err, stop backing off",
				"err", err)
			return nil, err
		} else {
			r.Log.Error("[Stream] Get err", "err", err)
		}
		to_wait = bk[0]
		bk = bk[1:]
	}
}

// Bulkhead pattern is used to limit the number of concurrent Get().
type BulkheadStream struct {
	Name      string
	Log       log15.Logger
	stream    Stream
	semaphore chan *struct{}
}

// Create a BulkheadStream that allows at maximum $max_concurrency Get() to
// run concurrently.
func NewBulkheadStream(name string, stream Stream, max_concurrency int) *BulkheadStream {
	if max_concurrency <= 0 {
		panic(errors.New("max_concurrent must be greater than 0"))
	}
	return &BulkheadStream{
		Name:      name,
		Log:       Log.New("mixin", "stream_bulk", "name", name),
		stream:    stream,
		semaphore: make(chan *struct{}, max_concurrency),
	}
}

// Get() is blocked when the limit is exceeded.
func (r *BulkheadStream) Get() (Message, error) {
	r.Log.Debug("[Stream] Get()")
	r.semaphore <- &struct{}{}
	defer func() { <-r.semaphore }()
	msg, err := r.stream.Get()
	if err == nil {
		r.Log.Debug("[Stream] Get ok", "msg_out", msg.Id())
	} else {
		r.Log.Error("[Stream] Get err", "err", err)
	}
	return msg, err
}

// CircuitBreakerStream is a Stream equipped with a circuit-breaker.
type CircuitBreakerStream struct {
	Name    string
	Log     log15.Logger
	Circuit string
	stream  Stream
}

// In hystrix-go, a circuit-breaker must be given a unique name.
// NewCircuitBreakerStream() creates a CircuitBreakerStream with a
// circuit-breaker named $circuit.
func NewCircuitBreakerStream(name string, stream Stream, circuit string) *CircuitBreakerStream {
	return &CircuitBreakerStream{
		Name:    name,
		Log: Log.New("mixin", "stream_circuit", "name", name,
			"circuit", circuit),
		Circuit: circuit,
		stream:  stream,
	}
}

func (r *CircuitBreakerStream) Get() (Message, error) {
	r.Log.Debug("[Stream] Get()")
	result := make(chan *tuple, 1)
	err := hystrix.Do(r.Circuit, func() error {
		msg, err := r.stream.Get()
		if err != nil {
			r.Log.Error("[Stream] Get err", "err", err)
			result <- &tuple{
				fst: msg,
				snd: err,
			}
			return err
		}
		r.Log.Debug("[Stream] Get ok", "msg_out", msg.Id())
		result <- &tuple{
			fst: msg,
			snd: err,
		}
		return nil
	}, nil)
	// hystrix.ErrTimeout doesn't interrupt work anyway.
	// It just contributes to circuit's metrics.
	if err != nil {
		r.Log.Warn("[Stream] Circuit err", "err", err)
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
	Log     log15.Logger
}

// Create a ChannelStream that gets Messages from $channel.
func NewChannelStream(name string, channel <-chan Message) *ChannelStream {
	return &ChannelStream{
		Name:    name,
		Log:     Log.New("mixin", "chanStream", "name", name),
		channel: channel,
	}
}

func (r *ChannelStream) Get() (Message, error) {
	r.Log.Debug("[Stream] Get()")
	msg := <-r.channel
	r.Log.Debug("[Stream] Get ok", "msg_out", msg.Id())
	return msg, nil
}

// ConcurrentFetchStream internally keeps fetching a number of items from
// upstream concurrently.
// Note that the order of messages emitted from the upstream may not be
// preserved. It's down to application to maintain the order if that's required.
type ConcurrentFetchStream struct {
	Name      string
	Log       log15.Logger
	stream    Stream
	receives  chan *tuple
	semaphore chan *struct{}
	once      sync.Once
}

// Create a ConcurrentFetchStream that allows at maximum $max_concurrency
// Messages being internally fetched from upstream concurrently.
func NewConcurrentFetchStream(name string, stream Stream, max_concurrency int) *ConcurrentFetchStream {
	return &ConcurrentFetchStream{
		Name:      name,
		Log:       Log.New("mixin", "fetchStream", "name", name),
		stream:    stream,
		receives:  make(chan *tuple, max_concurrency),
		semaphore: make(chan *struct{}, max_concurrency),
	}
}

func (r *ConcurrentFetchStream) onceDo() {
	go func() {
		r.Log.Info("[Stream] once")
		for {
			// pull more messages as long as semaphore allows
			r.semaphore <- &struct{}{}
			// Since Get() are run concurrently, the order of
			// elements from upstream may not preserved.
			go func() {
				r.Log.Debug("[Stream] onceDo Get()")
				msg, err := r.stream.Get()
				if err == nil {
					r.Log.Debug("[Stream] onceDo Get ok",
						"msg_out", msg.Id())
				} else {
					r.Log.Error("[Stream] onceDo Get err",
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
	r.Log.Debug("[Stream] Get()")
	r.once.Do(r.onceDo)
	tp := <-r.receives
	<-r.semaphore
	if tp.snd != nil {
		err := tp.snd.(error)
		r.Log.Error("[Stream] Get err", "err", err)
		return nil, err
	}
	msg := tp.fst.(Message)
	r.Log.Debug("[Stream] Get ok", "msg_out", msg.Id())
	return msg, nil
}

// A MappedStream whose Get() emits a Message transformed by a Handler from
// a given Stream.
type MappedStream struct {
	Name    string
	Log     log15.Logger
	stream  Stream
	handler Handler
}

func NewMappedStream(name string, stream Stream, handler Handler) *MappedStream {
	return &MappedStream{
		Name:    name,
		Log:     Log.New("mixin", "mappedStream", "name", name),
		stream:  stream,
		handler: handler,
	}
}

func (r *MappedStream) Get() (Message, error) {
	r.Log.Debug("[Stream] Get()")
	msg, err := r.stream.Get()
	if err != nil {
		r.Log.Error("[Stream] Get err", "err", err)
		return nil, err
	}
	r.Log.Debug("[Stream] Get ok, run Handle()", "msg", msg.Id())
	m, e := r.handler.Handle(msg)
	if e != nil {
		r.Log.Error("[Stream] Handle err", "err", err)
		return nil, e
	}
	r.Log.Debug("[Stream] Handle done", "msg_in", msg.Id(),
		"msg_out", m.Id())
	return m, nil
}
