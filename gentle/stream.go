package gentle

import (
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	log15 "gopkg.in/inconshreveable/log15.v2"
	prom "github.com/prometheus/client_golang/prometheus"
	"sync"
	"time"
)

const (
	// Stream types(mixins), are most likely used as part of RegistryKey.
	MIXIN_STREAM_RATELIMITED = "s_rate"
	MIXIN_STREAM_RETRY = "s_retry"
	MIXIN_STREAM_BULKHEAD = "s_bulk"
	MIXIN_STREAM_CIRCUITBREAKER = "s_circuit"
	MIXIN_STREAM_CHANNEL = "s_chan"
	MIXIN_STREAM_CONCURRENTFETCH = "s_con"
	MIXIN_STREAM_MAPPED = "s_map"
)

var (
	// Errors that CircuitBreakerStream, in addition to application's errors, might
	// return. They are replacement of hystrix errors. Get() won't return
	// any hystrix errors.
	ErrCircuitOpen = errors.New(hystrix.ErrCircuitOpen.Error())
	ErrMaxConcurrency = errors.New(hystrix.ErrMaxConcurrency.Error())
	ErrTimeout = errors.New(hystrix.ErrTimeout.Error())

	label_ok = map[string]string {"result": "ok"}
	label_err = map[string]string {"result": "err"}
)

// Rate limiting pattern is used to limit the speed of a series of Get().
type RateLimitedStream struct {
	Namespace string
	Name    string
	Log     log15.Logger
	stream  Stream
	limiter RateLimit
	getObservation Observation
}

func NewRateLimitedStream(namespace string, name string, stream Stream,
	limiter RateLimit) *RateLimitedStream {

	return &RateLimitedStream{
		Namespace: namespace,
		Name:    name,
		Log:     Log.New("namespace", namespace,
			"mixin", MIXIN_STREAM_RATELIMITED, "name", name),
		stream:  stream,
		limiter: limiter,
		getObservation: dummyObservationIfNonRegistered(
			RegistryKey{namespace,
				    MIXIN_STREAM_RATELIMITED,
				    name, "get"}),
	}
}

// Get() is blocked when the limit is exceeded.
func (r *RateLimitedStream) Get() (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Stream] Get() ...")
	r.limiter.Wait(1, 0)
	msg, err := r.stream.Get()
	if err != nil {
		r.Log.Error("[Stream] Get() err", "err", err)
		r.getObservation.Observe(time.Now().Sub(begin).Seconds(),
			label_err)
		return nil, err
	}
	r.Log.Debug("[Stream] Get() ok", "msg_out", msg.Id())
	r.getObservation.Observe(time.Now().Sub(begin).Seconds(),
		label_ok)
	return msg, nil
}

// RetryStream will, when Get() encounters error, back off for some time
// and then retries.
type RetryStream struct {
	Namespace string
	Name     string
	Log      log15.Logger
	stream   Stream
	backoffs []time.Duration
	getObservation	Observation
	tryObservation	Observation
}

func NewRetryStream(namespace string, name string, stream Stream,
	backoffs []time.Duration) *RetryStream {

	if len(backoffs) == 0 {
		Log.Warn("NewRetryStream() len(backoffs) == 0")
	}
	return &RetryStream{
		Namespace: namespace,
		Name:      name,
		Log:       Log.New("namespace", namespace, "mixin", MIXIN_STREAM_RETRY, "name", name),
		stream:    stream,
		backoffs:  backoffs,
		getObservation: dummyObservationIfNonRegistered(
			RegistryKey{namespace,
				    MIXIN_STREAM_RETRY,
				    name, "get"}),
		tryObservation: dummyObservationIfNonRegistered(
			RegistryKey{namespace,
				    MIXIN_STREAM_RETRY,
				    name, "try"}),
	}
}

func (r *RetryStream) Get() (Message, error) {
	begin := time.Now()
	bk := r.backoffs
	to_wait := 0 * time.Second
	for {
		count := len(r.backoffs)-len(bk)+1
		r.Log.Debug("[Stream] Get() ...", "count", count,
			"wait", to_wait)
		// A negative or zero duration causes Sleep to return immediately.
		time.Sleep(to_wait)
		// assert end_allowed.Sub(now) != 0
		msg, err := r.stream.Get()
		if err == nil {
			timespan := time.Now().Sub(begin).Seconds()
			r.Log.Debug("[Stream] Get() ok", "msg_out", msg.Id(),
				"timespan", timespan)
			r.getObservation.Observe(timespan, label_ok)
			r.tryObservation.Observe(float64(count), label_ok)
			return msg, nil
		}
		if len(bk) == 0 {
			timespan := time.Now().Sub(begin).Seconds()
			r.Log.Error("[Streamer] Get() err and no more backing off",
				"err", err, "timespan", timespan)
			r.getObservation.Observe(timespan, label_err)
			r.tryObservation.Observe(float64(count), label_err)
			return nil, err
		} else {
			timespan := time.Now().Sub(begin)
			r.Log.Error("[Stream] Get() err, backing off ...",
				"err", err, "timespan", timespan)
			to_wait = bk[0]
			bk = bk[1:]
		}
	}
}

// Bulkhead pattern is used to limit the number of concurrent Get().
type BulkheadStream struct {
	Namespace string
	Name      string
	Log       log15.Logger
	stream    Stream
	semaphore chan *struct{}
	getObservation Observation
}

// Create a BulkheadStream that allows at maximum $max_concurrency Get() to
// run concurrently.
func NewBulkheadStream(namespace string, name string, stream Stream,
	max_concurrency int) *BulkheadStream {

	if max_concurrency <= 0 {
		panic(errors.New("max_concurrent must be greater than 0"))
	}
	return &BulkheadStream{
		Namespace: namespace,
		Name:      name,
		Log:       Log.New("namespace", namespace,
			"mixin", MIXIN_STREAM_BULKHEAD, "name", name),
		stream:    stream,
		semaphore: make(chan *struct{}, max_concurrency),
		getObservation:	  dummyObservationIfNonRegistered(
			RegistryKey{namespace,
				    MIXIN_STREAM_BULKHEAD,
				    name, "get"}),
	}
}

// Get() is blocked when the limit is exceeded.
func (r *BulkheadStream) Get() (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Stream] Get() ...")
	r.semaphore <- &struct{}{}
	msg, err := r.stream.Get()
	if err != nil {
		r.Log.Error("[Stream] Get() err", "err", err)
		<-r.semaphore
		r.getObservation.Observe(time.Now().Sub(begin).Seconds(),
			label_err)
		return nil, err
	}
	r.Log.Debug("[Stream] Get() ok", "msg_out", msg.Id())
	<-r.semaphore
	r.getObservation.Observe(time.Now().Sub(begin).Seconds(),
		label_ok)
	return msg, nil
}

// CircuitBreakerStream is a Stream equipped with a circuit-breaker.
type CircuitBreakerStream struct {
	Namespace string
	Name    string
	Log     log15.Logger
	Circuit string
	stream  Stream
	getObservation	Observation
	errCounter Counter
}

// In hystrix-go, a circuit-breaker must be given a unique name.
// NewCircuitBreakerStream() creates a CircuitBreakerStream with a
// circuit-breaker named $circuit.
func NewCircuitBreakerStream(namespace string, name string, stream Stream,
	circuit string) *CircuitBreakerStream {

	return &CircuitBreakerStream{
		Namespace: namespace,
		Name: name,
		Log: Log.New("namespace", namespace, "mixin", MIXIN_STREAM_CIRCUITBREAKER,
			"name", name, "circuit", circuit),
		Circuit: circuit,
		stream:  stream,
		getObservation:	 dummyObservationIfNonRegistered(
			RegistryKey{namespace,
				MIXIN_STREAM_CIRCUITBREAKER,
				name, "get"}),
		errCounter:	 dummyCounterIfNonRegistered(
			RegistryKey{namespace,
				    MIXIN_STREAM_CIRCUITBREAKER,
				    name, "hystrix_err"}),
	}
}

func (r *CircuitBreakerStream) Get() (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Stream] Get() ...")
	result := make(chan Message, 1)
	err := hystrix.Do(r.Circuit, func() error {
		msg, err := r.stream.Get()
		if err != nil {
			r.Log.Error("[Stream] Get() err", "err", err)
			return err
		}
		r.Log.Debug("[Stream] Get() ok", "msg_out", msg.Id())
		result <- msg
		return nil
	}, nil)
	if err != nil {
		defer r.getObservation.Observe(time.Now().Sub(begin).Seconds(),
			label_err)
		// To prevent misinterpreting when wrapping one
		// CircuitBreakerStream over another. Hystrix errors are
		// replaced so that Get() won't return any hystrix errors.
		switch err {
		case hystrix.ErrCircuitOpen:
			r.Log.Warn("[Stream] Circuit err", "err", err)
			r.errCounter.Add(1,
				map[string]string{"err": "ErrCircuitOpen"})
			return nil, ErrCircuitOpen
		case hystrix.ErrMaxConcurrency:
			r.Log.Warn("[Stream] Circuit err", "err", err)
			r.errCounter.Add(1,
				map[string]string{"err": "ErrMaxConcurrency"})
			return nil, ErrMaxConcurrency
		case hystrix.ErrTimeout:
			r.Log.Warn("[Stream] Circuit err", "err", err)
			r.errCounter.Add(1,
				map[string]string{"err": "ErrTimeout"})
			return nil, ErrTimeout
		default:
			r.errCounter.Add(1,
				map[string]string{"err": "NonHystrixErr"})
			return nil, err
		}
	} else {
		msg := <-result
		r.getObservation.Observe(time.Now().Sub(begin).Seconds(),
			label_ok)
		return msg, nil
	}
}

// ChannelStream forms a stream from a channel.
type ChannelStream struct {
	Namespace string
	Name    string
	Log     log15.Logger
	channel <-chan Message
	getObservation Observation
}

// Create a ChannelStream that gets Messages from $channel.
func NewChannelStream(namespace string, name string,
	channel <-chan Message) *ChannelStream {

	return &ChannelStream{
		Namespace: namespace,
		Name:    name,
		Log:     Log.New("namespace", namespace, "mixin", MIXIN_STREAM_CHANNEL, "name", name),
		channel: channel,
		getObservation: dummyObservationIfNonRegistered(
			RegistryKey{namespace,
				    MIXIN_STREAM_CHANNEL,
				    name, "get"}),
	}
}

func (r *ChannelStream) Get() (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Stream] Get() ...")
	msg := <-r.channel
	r.Log.Debug("[Stream] Get() ok", "msg_out", msg.Id())
	r.getObservation.Observe(time.Now().Sub(begin).Seconds(), label_ok)
	return msg, nil
}

// ConcurrentFetchStream internally keeps fetching a number of items from
// upstream concurrently.
// Note that the order of messages emitted from the upstream may not be
// preserved. It's down to application to maintain the order if that's required.
type ConcurrentFetchStream struct {
	Namespace string
	Name      string
	Log       log15.Logger
	stream    Stream
	receives  chan *tuple
	semaphore chan *struct{}
	once      sync.Once
	getObservation Observation
}

// Create a ConcurrentFetchStream that allows at maximum $max_concurrency
// Messages being internally fetched from upstream concurrently.
func NewConcurrentFetchStream(namespace string, name string, stream Stream,
	max_concurrency int) *ConcurrentFetchStream {

	return &ConcurrentFetchStream{
		Namespace: namespace,
		Name:      name,
		Log:       Log.New("namespace", namespace, "mixin", MIXIN_STREAM_CONCURRENTFETCH, "name", name),
		stream:    stream,
		receives:  make(chan *tuple, max_concurrency),
		semaphore: make(chan *struct{}, max_concurrency),
		getObservation: dummyObservationIfNonRegistered(
			RegistryKey{namespace,
				    MIXIN_STREAM_CONCURRENTFETCH,
				    name, "get"}),
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
				r.Log.Debug("[Stream] onceDo Get() ...")
				msg, err := r.stream.Get()
				if err == nil {
					r.Log.Debug("[Stream] onceDo Get() ok",
						"msg_out", msg.Id())
				} else {
					r.Log.Error("[Stream] onceDo Get() err",
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
	begin := time.Now()
	r.Log.Debug("[Stream] Get() ...")
	r.once.Do(r.onceDo)
	tp := <-r.receives
	<-r.semaphore
	if tp.snd != nil {
		err := tp.snd.(error)
		r.Log.Error("[Stream] Get() err", "err", err)
		r.getObservation.Observe(time.Now().Sub(begin).Seconds(),
			label_err)
		return nil, err
	}
	msg := tp.fst.(Message)
	r.Log.Debug("[Stream] Get() ok", "msg_out", msg.Id())
	r.getObservation.Observe(time.Now().Sub(begin).Seconds(), label_ok)
	return msg, nil
}

// A MappedStream whose Get() emits a Message transformed by a Handler from
// a given Stream.
type MappedStream struct {
	Namespace string
	Name    string
	Log     log15.Logger
	stream  Stream
	handler Handler
	getObservation Observation
}

func NewMappedStream(namespace string, name string, stream Stream, handler Handler) *MappedStream {
	return &MappedStream{
		Namespace: namespace,
		Name:    name,
		Log:     Log.New("namespace", namespace, "mixin", MIXIN_STREAM_MAPPED, "name", name),
		stream:  stream,
		handler: handler,
		getObservation: dummyObservationIfNonRegistered(
			RegistryKey{namespace,
				    MIXIN_STREAM_MAPPED,
				    name, "get"}),
	}
}

func (r *MappedStream) Get() (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Stream] Get() ...")
	msg, err := r.stream.Get()
	if err != nil {
		r.Log.Error("[Stream] Get() err", "err", err)
		r.getObservation.Observe(time.Now().Sub(begin).Seconds(), label_err)
		return nil, err
	}
	r.Log.Debug("[Stream] Get() ok, Handle() ...", "msg", msg.Id())
	hmsg, herr := r.handler.Handle(msg)
	if herr != nil {
		r.Log.Error("[Stream] Handle() err", "err", herr)
		r.getObservation.Observe(time.Now().Sub(begin).Seconds(), label_err)
		return nil, herr
	}
	r.Log.Debug("[Stream] Handle() ok", "msg_in", msg.Id(),
		"msg_out", hmsg.Id())
	r.getObservation.Observe(time.Now().Sub(begin).Seconds(), label_ok)
	return hmsg, nil
}
