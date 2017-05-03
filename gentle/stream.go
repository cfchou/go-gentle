package gentle

import (
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	log15 "gopkg.in/inconshreveable/log15.v2"
	"sync"
	"time"
)

const (
	// Stream types(mixins), are most often used as part of RegistryKey.
	MIXIN_STREAM_RATELIMITED     = "sRate"
	MIXIN_STREAM_RETRY           = "sRetry"
	MIXIN_STREAM_BULKHEAD        = "sBulk"
	MIXIN_STREAM_CIRCUITBREAKER  = "sCircuit"
	MIXIN_STREAM_CHANNEL         = "sChan"
	MIXIN_STREAM_CONCURRENTFETCH = "sCon"
	MIXIN_STREAM_HANDLED         = "sHan"
	MIXIN_STREAM_TRANS           = "sTrans"
)

var (
	label_ok  = map[string]string{"result": "ok"}
	label_err = map[string]string{"result": "err"}
)

// Rate limiting pattern is used to limit the speed of a series of Get().
type RateLimitedStream struct {
	Namespace      string
	Name           string
	Log            log15.Logger
	stream         Stream
	limiter        RateLimit
	getObservation Observation
}

func NewRateLimitedStream(namespace, name string, stream Stream,
	limiter RateLimit) *RateLimitedStream {

	return &RateLimitedStream{
		Namespace: namespace,
		Name:      name,
		Log: Log.New("namespace", namespace,
			"mixin", MIXIN_STREAM_RATELIMITED, "name", name),
		stream:  stream,
		limiter: limiter,
		getObservation: NoOpObservationIfNonRegistered(
			&RegistryKey{namespace,
				MIXIN_STREAM_RATELIMITED,
				name, MX_STREAM_GET}),
	}
}

// Get() is blocked when the limit is exceeded.
func (r *RateLimitedStream) Get() (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Stream] Get() ...")
	r.limiter.Wait(1, 0)
	msg, err := r.stream.Get()
	timespan := time.Now().Sub(begin).Seconds()
	if err != nil {
		r.Log.Error("[Stream] Get() err", "err", err,
			"timespan", timespan)
		r.getObservation.Observe(timespan, label_err)
		return nil, err
	}
	r.Log.Debug("[Stream] Get() ok", "msg_out", msg.Id(),
		"timespan", timespan)
	r.getObservation.Observe(timespan, label_ok)
	return msg, nil
}

// RetryStream will, when Get() encounters error, back off for some time
// and then retries.
type RetryStream struct {
	Namespace      string
	Name           string
	Log            log15.Logger
	stream         Stream
	backoffs       []time.Duration
	getObservation Observation
	tryObservation Observation
}

func NewRetryStream(namespace string, name string, stream Stream,
	backoffs []time.Duration) *RetryStream {

	if len(backoffs) == 0 {
		Log.Warn("NewRetryStream() len(backoffs) == 0")
	}
	return &RetryStream{
		Namespace: namespace,
		Name:      name,
		Log: Log.New("namespace", namespace, "mixin",
			MIXIN_STREAM_RETRY, "name", name),
		stream:   stream,
		backoffs: backoffs,
		getObservation: NoOpObservationIfNonRegistered(
			&RegistryKey{namespace,
				MIXIN_STREAM_RETRY,
				name, MX_STREAM_GET}),
		tryObservation: NoOpObservationIfNonRegistered(
			&RegistryKey{namespace,
				MIXIN_STREAM_RETRY,
				name, MX_STREAM_RETRY_TRY}),
	}
}

func (r *RetryStream) Get() (Message, error) {
	begin := time.Now()
	bk := r.backoffs
	to_wait := 0 * time.Second
	for {
		count := len(r.backoffs) - len(bk) + 1
		r.Log.Debug("[Stream] Get() ...", "count", count,
			"wait", to_wait)
		time.Sleep(to_wait)
		msg, err := r.stream.Get()
		timespan := time.Now().Sub(begin).Seconds()
		if err == nil {
			r.Log.Debug("[Stream] Get() ok", "msg_out", msg.Id(),
				"timespan", timespan)
			r.getObservation.Observe(timespan, label_ok)
			r.tryObservation.Observe(float64(count), label_ok)
			return msg, nil
		}
		if len(bk) == 0 {
			r.Log.Error("[Streamer] Get() err and no more backing off",
				"err", err, "timespan", timespan)
			r.getObservation.Observe(timespan, label_err)
			r.tryObservation.Observe(float64(count), label_err)
			return nil, err
		} else {
			r.Log.Error("[Stream] Get() err, backing off ...",
				"err", err, "timespan", timespan)
			to_wait = bk[0]
			bk = bk[1:]
		}
	}
}

// Bulkhead pattern is used to limit the number of concurrently hanging Get().
// It uses semaphore isolation, similar to the approach used in hystrix.
// http://stackoverflow.com/questions/30391809/what-is-bulkhead-pattern-used-by-hystrix
type BulkheadStream struct {
	Namespace      string
	Name           string
	Log            log15.Logger
	stream         Stream
	semaphore      chan *struct{}
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
		Log: Log.New("namespace", namespace,
			"mixin", MIXIN_STREAM_BULKHEAD, "name", name),
		stream:    stream,
		semaphore: make(chan *struct{}, max_concurrency),
		getObservation: NoOpObservationIfNonRegistered(
			&RegistryKey{namespace,
				MIXIN_STREAM_BULKHEAD,
				name, MX_STREAM_GET}),
	}
}

// Get() is blocked when the limit is exceeded.
func (r *BulkheadStream) Get() (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Stream] Get() ...")
	select {
	case r.semaphore <- &struct{}{}:
		defer func() {
			<-r.semaphore
		}()
		msg, err := r.stream.Get()
		timespan := time.Now().Sub(begin).Seconds()
		if err != nil {
			r.Log.Error("[Stream] Get() err", "err", err,
				"timespan", timespan)
			r.getObservation.Observe(timespan, label_err)
			return nil, err
		}
		r.Log.Debug("[Stream] Get() ok", "msg_out", msg.Id(),
			"timespan", timespan)
		r.getObservation.Observe(timespan, label_ok)
		return msg, nil
	default:
		r.Log.Error("[Stream] Get() err", "err", ErrMaxConcurrency)
		return nil, ErrMaxConcurrency
	}
}

// CircuitBreakerStream is a Stream equipped with a circuit-breaker.
type CircuitBreakerStream struct {
	Namespace      string
	Name           string
	Log            log15.Logger
	Circuit        string
	stream         Stream
	getObservation Observation
	errCounter     Observation
}

// In hystrix-go, a circuit-breaker must be given a unique name.
// NewCircuitBreakerStream() creates a CircuitBreakerStream with a
// circuit-breaker named $circuit.
func NewCircuitBreakerStream(namespace string, name string, stream Stream,
	circuit string) *CircuitBreakerStream {

	return &CircuitBreakerStream{
		Namespace: namespace,
		Name:      name,
		Log: Log.New("namespace", namespace,
			"mixin", MIXIN_STREAM_CIRCUITBREAKER,
			"name", name, "circuit", circuit),
		Circuit: circuit,
		stream:  stream,
		getObservation: NoOpObservationIfNonRegistered(
			&RegistryKey{namespace,
				MIXIN_STREAM_CIRCUITBREAKER,
				name, MX_STREAM_GET}),
		errCounter: NoOpObservationIfNonRegistered(
			&RegistryKey{namespace,
				MIXIN_STREAM_CIRCUITBREAKER,
				name, MX_STREAM_CIRCUITBREAKER_HXERR}),
	}
}

func (r *CircuitBreakerStream) Get() (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Stream] Get() ...")
	result := make(chan Message, 1)
	err := hystrix.Do(r.Circuit, func() error {
		msg, err := r.stream.Get()
		timespan := time.Now().Sub(begin).Seconds()
		if err != nil {
			r.Log.Error("[Stream] Get() in CB err",
				"err", err, "timespan", timespan)
			return err
		}
		r.Log.Debug("[Stream] Get() in CB ok",
			"msg_out", msg.Id(), "timespan", timespan)
		result <- msg
		return nil
	}, nil)
	if err != nil {
		defer func() {
			timespan := time.Now().Sub(begin).Seconds()
			r.Log.Error("[Stream] Circuit err", "err", err,
				"timespan", timespan)
			r.getObservation.Observe(timespan, label_err)
		}()
		// To prevent misinterpreting when wrapping one
		// CircuitBreakerStream over another. Hystrix errors are
		// replaced so that Get() won't return any hystrix errors.
		switch err {
		case hystrix.ErrCircuitOpen:
			r.errCounter.Observe(1,
				map[string]string{"err": "ErrCbOpen"})
			return nil, ErrCbOpen
		case hystrix.ErrMaxConcurrency:
			r.errCounter.Observe(1,
				map[string]string{"err": "ErrCbMaxConcurrency"})
			return nil, ErrCbMaxConcurrency
		case hystrix.ErrTimeout:
			r.errCounter.Observe(1,
				map[string]string{"err": "ErrCbTimeout"})
			return nil, ErrCbTimeout
		default:
			r.errCounter.Observe(1,
				map[string]string{"err": "NonCbErr"})
			return nil, err
		}
	}
	msg := <-result
	timespan := time.Now().Sub(begin).Seconds()
	r.Log.Debug("[Stream] Get() ok", "msg_out", msg.Id(),
		"timespan", timespan)
	r.getObservation.Observe(timespan, label_ok)
	return msg, nil
}

// ChannelStream forms a stream from a channel.
type ChannelStream struct {
	Namespace      string
	Name           string
	Log            log15.Logger
	channel        <-chan interface{}
	getObservation Observation
}

// Create a ChannelStream that gets Messages from $channel.
func NewChannelStream(namespace string, name string,
	channel <-chan interface{}) *ChannelStream {

	return &ChannelStream{
		Namespace: namespace,
		Name:      name,
		Log:       Log.New("namespace", namespace, "mixin", MIXIN_STREAM_CHANNEL, "name", name),
		channel:   channel,
		getObservation: NoOpObservationIfNonRegistered(
			&RegistryKey{namespace,
				MIXIN_STREAM_CHANNEL,
				name, MX_STREAM_GET}),
	}
}

func (r *ChannelStream) Get() (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Stream] Get() ...")
	switch v := (<-r.channel).(type) {
	case Message:
		timespan := time.Now().Sub(begin).Seconds()
		r.Log.Debug("[Stream] Get() ok", "msg_out", v.Id(),
			"timespan", timespan)
		r.getObservation.Observe(timespan, label_ok)
		return v, nil
	case error:
		timespan := time.Now().Sub(begin).Seconds()
		r.Log.Debug("[Stream] Get() err", "err", v,
			"timespan", timespan)
		r.getObservation.Observe(timespan, label_err)
		return nil, v
	default:
		r.Log.Error("[Stream] Get() err, invalid type",
			"value", v)
		return nil, ErrInvalidType
	}
}

// ConcurrentFetchStream internally keeps fetching a number of items from
// upstream concurrently.
// Note that the order of messages emitted from the upstream may not be
// preserved. It's down to application to maintain the order if that's required.
type ConcurrentFetchStream struct {
	Namespace      string
	Name           string
	Log            log15.Logger
	stream         Stream
	receives       chan *tuple
	semaphore      chan *struct{}
	once           sync.Once
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
		getObservation: NoOpObservationIfNonRegistered(
			&RegistryKey{namespace,
				MIXIN_STREAM_CONCURRENTFETCH,
				name, MX_STREAM_GET}),
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
	timespan := time.Now().Sub(begin).Seconds()
	if tp.snd != nil {
		err := tp.snd.(error)
		r.Log.Error("[Stream] Get() err", "err", err,
			"timespan", timespan)
		r.getObservation.Observe(timespan, label_err)
		return nil, err
	}
	msg := tp.fst.(Message)
	r.Log.Debug("[Stream] Get() ok", "msg_out", msg.Id(),
		"timespan", timespan)
	r.getObservation.Observe(timespan, label_ok)
	return msg, nil
}

// A HandlerStream whose Get() emits a Message transformed by a Handler from
// a given Stream.
type HandlerStream struct {
	Namespace      string
	Name           string
	Log            log15.Logger
	stream         Stream
	handler        Handler
	getObservation Observation
}

func NewHandlerStream(namespace string, name string, stream Stream, handler Handler) *HandlerStream {
	return &HandlerStream{
		Namespace: namespace,
		Name:      name,
		Log:       Log.New("namespace", namespace, "mixin", MIXIN_STREAM_HANDLED, "name", name),
		stream:    stream,
		handler:   handler,
		getObservation: NoOpObservationIfNonRegistered(
			&RegistryKey{namespace,
				MIXIN_STREAM_HANDLED,
				name, MX_STREAM_GET}),
	}
}

func (r *HandlerStream) Get() (Message, error) {
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
	timespan := time.Now().Sub(begin).Seconds()
	if herr != nil {
		r.Log.Error("[Stream] Handle() err", "err", herr,
			"timespan", timespan)
		r.getObservation.Observe(timespan, label_err)
		return nil, herr
	}
	r.Log.Debug("[Stream] Handle() ok", "msg_in", msg.Id(),
		"msg_out", hmsg.Id(), "timespan", timespan)
	r.getObservation.Observe(timespan, label_ok)
	return hmsg, nil
}

// TransformStream transforms what Stream.Get() returns.
type TransformStream struct {
	Namespace      string
	Name           string
	Log            log15.Logger
	stream         Stream
	transFunc      func(Message, error) (Message, error)
	getObservation Observation
}

func NewTransformStream(namespace string, name string, stream Stream,
	transFunc func(Message, error) (Message, error)) *TransformStream {

	return &TransformStream{
		Namespace: namespace,
		Name:      name,
		Log:       Log.New("namespace", namespace, "mixin", MIXIN_STREAM_TRANS, "name", name),
		stream:    stream,
		transFunc: transFunc,
		getObservation: NoOpObservationIfNonRegistered(
			&RegistryKey{namespace,
				MIXIN_STREAM_TRANS,
				name, MX_STREAM_GET}),
	}
}

func (r *TransformStream) Get() (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Stream] Get() ...")
	msg_mid, err := r.stream.Get()
	if err != nil {
		r.Log.Debug("[Stream] Get() err, transFunc() ...",
			"err", err)
		// enforce the exclusivity
		msg_mid = nil
	} else {
		r.Log.Debug("[Stream] Get() ok, transFunc() ...",
			"msg_mid", msg_mid.Id())
	}
	msg_out, err2 := r.transFunc(msg_mid, err)
	timespan := time.Now().Sub(begin).Seconds()
	if err2 != nil {
		if msg_mid != nil {
			r.Log.Error("[Stream] transFunc() err",
				"msg_mid", msg_mid.Id(), "err", err2,
				"timespan", timespan)
		} else {
			r.Log.Error("[Stream] transFunc() err",
				"err", err2, "timespan", timespan)
		}
		r.getObservation.Observe(timespan, label_err)
		return nil, err2
	}
	if msg_mid != nil {
		r.Log.Debug("[Stream] transFunc() ok",
			"msg_mid", msg_mid.Id(),
			"msg_out", msg_out.Id(), "timespan", timespan)
	} else {
		r.Log.Debug("[Stream] transFunc() ok",
			"msg_out", msg_out.Id(), "timespan", timespan)

	}
	r.getObservation.Observe(timespan, label_ok)
	return msg_out, nil
}
