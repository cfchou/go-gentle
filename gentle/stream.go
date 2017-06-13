package gentle

import (
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/benbjohnson/clock"
	"sync"
	"time"
)

const (
	// Stream types(mixins), are most often used as part of RegistryKey.
	MIXIN_STREAM_RATELIMITED    = "sRate"
	MIXIN_STREAM_RETRY          = "sRetry"
	MIXIN_STREAM_BULKHEAD       = "sBulk"
	MIXIN_STREAM_SEMAPHORE      = "sSem"
	MIXIN_STREAM_CIRCUITBREAKER = "sCircuit"
	MIXIN_STREAM_CHANNEL        = "sChan"
	MIXIN_STREAM_HANDLED        = "sHan"
	MIXIN_STREAM_FALLBACK       = "sFb"
)

var (
	label_ok          = map[string]string{"result": "ok"}
	label_err         = map[string]string{"result": "err"}
	label_err_ignored = map[string]string{"result": "ierr"}
)

// Common options for XXXStreamOpts
type StreamOpts struct {
	Namespace string
	Name      string
	Log       Logger
	MetricGet Metric
}

// Common fields for XXXStream
type streamFields struct {
	namespace string
	name      string
	log       Logger
	mxGet     Metric
}

func newStreamFields(opts *StreamOpts) *streamFields {
	return &streamFields{
		namespace: opts.Namespace,
		name:      opts.Name,
		log:       opts.Log,
		mxGet:     opts.MetricGet,
	}
}

type RateLimitedStreamOpts struct {
	StreamOpts
	Limiter RateLimit
}

func NewRateLimitedStreamOpts(namespace, name string, limiter RateLimit) *RateLimitedStreamOpts {
	return &RateLimitedStreamOpts{
		StreamOpts: StreamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"mixin", MIXIN_STREAM_RATELIMITED, "name", name),
			MetricGet: noopMetric,
		},
		Limiter: limiter,
	}
}

// Rate limiting pattern is used to limit the speed of a series of Get().
type RateLimitedStream struct {
	streamFields
	limiter RateLimit
	stream  Stream
}

func NewRateLimitedStream(opts RateLimitedStreamOpts, upstream Stream) *RateLimitedStream {
	return &RateLimitedStream{
		streamFields: *newStreamFields(&opts.StreamOpts),
		limiter:      opts.Limiter,
		stream:       upstream,
	}
}

// Get() is blocked when the limit is exceeded.
func (r *RateLimitedStream) Get() (Message, error) {
	begin := time.Now()
	r.log.Debug("[Stream] Get() ...")
	r.limiter.Wait(1, 0)
	msg, err := r.stream.Get()
	timespan := time.Now().Sub(begin).Seconds()
	if err != nil {
		r.log.Error("[Stream] Get() err", "err", err,
			"timespan", timespan)
		r.mxGet.Observe(timespan, label_err)
		return nil, err
	}
	r.log.Debug("[Stream] Get() ok", "msg_out", msg.Id(),
		"timespan", timespan)
	r.mxGet.Observe(timespan, label_ok)
	return msg, nil
}

func (r *RateLimitedStream) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin:     MIXIN_STREAM_RATELIMITED,
		Name:      r.name,
	}
}

type RetryStreamOpts struct {
	StreamOpts
	MetricTryNum   Metric
	Clock          Clock
	BackOffFactory BackOffFactory
}

func NewRetryStreamOpts(namespace, name string, backOffFactory BackOffFactory) *RetryStreamOpts {
	return &RetryStreamOpts{
		StreamOpts: StreamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace, "mixin",
				MIXIN_STREAM_RETRY, "name", name),
			MetricGet: noopMetric,
		},
		MetricTryNum:   noopMetric,
		Clock:          clock.New(),
		BackOffFactory: backOffFactory,
	}
}

// RetryStream will, when Get() encounters error, back off for some time
// and then retries.
type RetryStream struct {
	streamFields
	obTryNum       Metric
	clock          Clock
	backOffFactory BackOffFactory
	stream         Stream
}

func NewRetryStream(opts RetryStreamOpts, upstream Stream) *RetryStream {
	return &RetryStream{
		streamFields:   *newStreamFields(&opts.StreamOpts),
		obTryNum:       opts.MetricTryNum,
		clock:          opts.Clock,
		backOffFactory: opts.BackOffFactory,
		stream:         upstream,
	}
}

func (r *RetryStream) Get() (Message, error) {
	begin := r.clock.Now()
	count := 1
	r.log.Debug("[Stream] Get() ...", "count", count)
	var once sync.Once
	var backOff BackOff
	for {
		msg, err := r.stream.Get()
		if err == nil {
			timespan := r.clock.Now().Sub(begin).Seconds()
			r.log.Debug("[Stream] Get() ok", "msg_out", msg.Id(),
				"timespan", timespan, "count", count)
			r.mxGet.Observe(timespan, label_ok)
			r.obTryNum.Observe(float64(count), label_ok)
			return msg, nil
		}
		if ToIgnore(err) {
			timespan := r.clock.Now().Sub(begin).Seconds()
			r.log.Debug("[Streamer] Get() err ignored",
				"err", err, "timespan", timespan,
				"count", count)
			r.mxGet.Observe(timespan, label_err_ignored)
			r.obTryNum.Observe(float64(count), label_ok)
			return nil, err
		}
		once.Do(func() {
			backOff = r.backOffFactory.NewBackOff()
		})
		to_wait := backOff.Next()
		// Next() should immediately return but we can't guarantee so
		// timespan is calculated after Next().
		timespan := r.clock.Now().Sub(begin).Seconds()
		if to_wait == BackOffStop {
			r.log.Error("[Streamer] Get() err and no more backing off",
				"err", err, "timespan", timespan,
				"count", count)
			r.mxGet.Observe(timespan, label_err)
			r.obTryNum.Observe(float64(count), label_err)
			return nil, err
		}
		// timespan in our convention is used to track the overall
		// time of current function. Here we record time
		// passed as "elapsed".
		count++
		r.log.Error("[Stream] Get() err, backing off ...",
			"err", err, "elapsed", timespan, "count", count,
			"wait", to_wait)
		r.clock.Sleep(to_wait)
	}
}

func (r *RetryStream) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin:     MIXIN_STREAM_RETRY,
		Name:      r.name,
	}
}

type BulkheadStreamOpts struct {
	StreamOpts
	MaxConcurrency int
}

func NewBulkheadStreamOpts(namespace, name string, max_concurrency int) *BulkheadStreamOpts {
	if max_concurrency <= 0 {
		panic(errors.New("max_concurrent must be greater than 0"))
	}

	return &BulkheadStreamOpts{
		StreamOpts: StreamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace, "mixin",
				MIXIN_STREAM_BULKHEAD, "name", name),
			MetricGet: noopMetric,
		},
		MaxConcurrency: max_concurrency,
	}
}

// Bulkhead pattern is used to limit the number of concurrently hanging Get().
// It uses semaphore isolation, similar to the approach used in hystrix.
// http://stackoverflow.com/questions/30391809/what-is-bulkhead-pattern-used-by-hystrix
type BulkheadStream struct {
	streamFields
	stream    Stream
	semaphore chan struct{}
}

// Create a BulkheadStream that allows at maximum $max_concurrency Get() to
// run concurrently.
func NewBulkheadStream(opts BulkheadStreamOpts, upstream Stream) *BulkheadStream {

	return &BulkheadStream{
		streamFields: *newStreamFields(&opts.StreamOpts),
		stream:       upstream,
		semaphore:    make(chan struct{}, opts.MaxConcurrency),
	}
}

// Get() is blocked when the limit is exceeded.
func (r *BulkheadStream) Get() (Message, error) {
	begin := time.Now()
	r.log.Debug("[Stream] Get() ...")
	select {
	case r.semaphore <- struct{}{}:
		defer func() {
			<-r.semaphore
		}()
		msg, err := r.stream.Get()
		timespan := time.Now().Sub(begin).Seconds()
		if err != nil {
			r.log.Error("[Stream] Get() err", "err", err,
				"timespan", timespan)
			r.mxGet.Observe(timespan, label_err)
			return nil, err
		}
		r.log.Debug("[Stream] Get() ok", "msg_out", msg.Id(),
			"timespan", timespan)
		r.mxGet.Observe(timespan, label_ok)
		return msg, nil
	default:
		r.log.Error("[Stream] Get() err", "err", ErrMaxConcurrency)
		return nil, ErrMaxConcurrency
	}
}

func (r *BulkheadStream) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin:     MIXIN_STREAM_BULKHEAD,
		Name:      r.name,
	}
}

func (r *BulkheadStream) GetMaxConcurrency() int {
	return cap(r.semaphore)
}

func (r *BulkheadStream) GetCurrentConcurrency() int {
	return len(r.semaphore)
}

type SemaphoreStreamOpts struct {
	StreamOpts
	MaxConcurrency int
}

func NewSemaphoreStreamOpts(namespace, name string, max_concurrency int) *SemaphoreStreamOpts {
	if max_concurrency <= 0 {
		panic(errors.New("max_concurrent must be greater than 0"))
	}

	return &SemaphoreStreamOpts{
		StreamOpts: StreamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace, "mixin",
				MIXIN_STREAM_SEMAPHORE, "name", name),
			MetricGet: noopMetric,
		},
		MaxConcurrency: max_concurrency,
	}
}

// It allows at maximum $max_concurrency Get() to run concurrently. Similar
// to Bulkhead, but it blocks when MaxConcurrency is reached.
type SemaphoreStream struct {
	streamFields
	stream    Stream
	semaphore chan struct{}
}

func NewSemaphoreStream(opts SemaphoreStreamOpts, upstream Stream) *SemaphoreStream {

	return &SemaphoreStream{
		streamFields: *newStreamFields(&opts.StreamOpts),
		stream:       upstream,
		semaphore:    make(chan struct{}, opts.MaxConcurrency),
	}
}

func (r *SemaphoreStream) Get() (Message, error) {
	begin := time.Now()
	r.log.Debug("[Stream] Get() ...")
	r.semaphore <- struct{}{}
	defer func() { <-r.semaphore }()
	msg, err := r.stream.Get()
	timespan := time.Now().Sub(begin).Seconds()
	if err != nil {
		r.log.Error("[Stream] Get() err", "err", err,
			"timespan", timespan)
		r.mxGet.Observe(timespan, label_err)
		return nil, err
	}
	r.log.Debug("[Stream] Get() ok", "msg_out", msg.Id(),
		"timespan", timespan)
	r.mxGet.Observe(timespan, label_ok)
	return msg, nil
}

func (r *SemaphoreStream) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin:     MIXIN_STREAM_SEMAPHORE,
		Name:      r.name,
	}
}

func (r *SemaphoreStream) GetMaxConcurrency() int {
	return cap(r.semaphore)
}

func (r *SemaphoreStream) GetCurrentConcurrency() int {
	return len(r.semaphore)
}

type CircuitBreakerStreamOpts struct {
	StreamOpts
	MetricCbErr Metric
	Circuit     string
}

func NewCircuitBreakerStreamOpts(namespace, name, circuit string) *CircuitBreakerStreamOpts {
	return &CircuitBreakerStreamOpts{
		StreamOpts: StreamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"mixin", MIXIN_STREAM_CIRCUITBREAKER,
				"name", name, "circuit", circuit),
			MetricGet: noopMetric,
		},
		MetricCbErr: noopMetric,
		Circuit:     circuit,
	}
}

// CircuitBreakerStream is a Stream equipped with a circuit-breaker.
type CircuitBreakerStream struct {
	streamFields
	mxCbErr Metric
	circuit string
	stream  Stream
}

// In hystrix-go, a circuit-breaker must be given a unique name.
// NewCircuitBreakerStream() creates a CircuitBreakerStream with a
// circuit-breaker named $circuit.
func NewCircuitBreakerStream(opts CircuitBreakerStreamOpts, stream Stream) *CircuitBreakerStream {

	// Note that if it might overwrite or be overwritten by concurrently
	// registering the same circuit.
	allCircuits := hystrix.GetCircuitSettings()
	if _, ok := allCircuits[opts.Circuit]; !ok {
		NewDefaultCircuitBreakerConf().RegisterFor(opts.Circuit)
	}

	return &CircuitBreakerStream{
		streamFields: *newStreamFields(&opts.StreamOpts),
		mxCbErr:      opts.MetricCbErr,
		circuit:      opts.Circuit,
		stream:       stream,
	}
}

func (r *CircuitBreakerStream) Get() (Message, error) {
	begin := time.Now()
	r.log.Debug("[Stream] Get() ...")
	result := make(chan interface{}, 1)
	err := hystrix.Do(r.circuit, func() error {
		msg, err := r.stream.Get()
		timespan := time.Now().Sub(begin).Seconds()
		if err != nil {
			if !ToIgnore(err) {
				r.log.Error("[Stream] Get() in CB err",
					"err", err, "timespan", timespan)
				return err
			}
			// faking a success to bypass hystrix's error metrics
			result <- err
			return nil
		}
		r.log.Debug("[Stream] Get() in CB ok",
			"msg_out", msg.Id(), "timespan", timespan)
		result <- msg
		return nil
	}, nil)
	// hystrix errors can overwrite stream.Get()'s err
	// hystrix.ErrTimeout doesn't interrupt work anyway.
	// It just contributes to circuit's metrics.
	if err != nil {
		defer func() {
			timespan := time.Now().Sub(begin).Seconds()
			r.log.Error("[Stream] Circuit err", "err", err,
				"timespan", timespan)
			r.mxGet.Observe(timespan, label_err)
		}()
		// To prevent misinterpreting when wrapping one
		// CircuitBreakerStream over another. Hystrix errors are
		// replaced so that Get() won't return any hystrix errors.
		switch err {
		case hystrix.ErrCircuitOpen:
			r.mxCbErr.Observe(1,
				map[string]string{"err": "ErrCbOpen"})
			return nil, ErrCbOpen
		case hystrix.ErrMaxConcurrency:
			r.mxCbErr.Observe(1,
				map[string]string{"err": "ErrCbMaxConcurrency"})
			return nil, ErrCbMaxConcurrency
		case hystrix.ErrTimeout:
			r.mxCbErr.Observe(1,
				map[string]string{"err": "ErrCbTimeout"})
			return nil, ErrCbTimeout
		default:
			r.mxCbErr.Observe(1,
				map[string]string{"err": "NonCbErr"})
			return nil, err
		}
	}
	switch v := (<-result).(type) {
	case error:
		timespan := time.Now().Sub(begin).Seconds()
		r.log.Debug("[Stream] Get() in CB err ignored",
			"err", v, "timespan", timespan)
		r.mxCbErr.Observe(1,
			map[string]string{"err": "NonCbErr"})
		r.mxGet.Observe(timespan, label_err_ignored)
		return nil, v
	case Message:
		timespan := time.Now().Sub(begin).Seconds()
		r.log.Debug("[Stream] Get() ok", "msg_out", v.Id(),
			"timespan", timespan)
		r.mxGet.Observe(timespan, label_ok)
		return v, nil
	default:
		panic("Never be here")
	}
}

func (r *CircuitBreakerStream) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin:     MIXIN_STREAM_CIRCUITBREAKER,
		Name:      r.name,
	}
}

func (r *CircuitBreakerStream) GetCircuitName() string {
	return r.circuit
}

type FallbackStreamOpts struct {
	StreamOpts
	FallbackFunc func(error) (Message, error)
}

func NewFallbackStreamOpts(namespace, name string,
	fallbackFunc func(error) (Message, error)) *FallbackStreamOpts {
	return &FallbackStreamOpts{
		StreamOpts: StreamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"mixin", MIXIN_STREAM_FALLBACK, "name", name),
			MetricGet: noopMetric,
		},
		FallbackFunc: fallbackFunc,
	}
}

// FallbackStream transforms what Stream.Get() returns.
type FallbackStream struct {
	streamFields
	fallbackFunc func(error) (Message, error)
	stream       Stream
}

func NewFallbackStream(opts FallbackStreamOpts, upstream Stream) *FallbackStream {
	return &FallbackStream{
		streamFields: *newStreamFields(&opts.StreamOpts),
		fallbackFunc: opts.FallbackFunc,
		stream:       upstream,
	}
}

func (r *FallbackStream) Get() (Message, error) {
	begin := time.Now()
	r.log.Debug("[Stream] Get() ...")
	msg, err := r.stream.Get()
	if err == nil {
		timespan := time.Now().Sub(begin).Seconds()
		r.log.Debug("[Stream] Get() ok, skip fallbackFunc",
			"msg", msg.Id(), "timespan", timespan)
		r.mxGet.Observe(timespan, label_ok)
		return msg, nil
	}
	r.log.Error("[Stream] Get() err, fallbackFunc() ...", "err", err)
	// fallback to deal with the err
	msg, err = r.fallbackFunc(err)
	timespan := time.Now().Sub(begin).Seconds()
	if err != nil {
		r.log.Error("[Stream] fallbackFunc() err",
			"err", err, "timespan", timespan)
		r.mxGet.Observe(timespan, label_err)
		return nil, err
	}
	r.log.Debug("[Stream] fallbackFunc() ok",
		"msg", msg.Id(), "timespan", timespan)
	r.mxGet.Observe(timespan, label_ok)
	return msg, nil
}

func (r *FallbackStream) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin:     MIXIN_STREAM_FALLBACK,
		Name:      r.name,
	}
}

type ChannelStreamOpts struct {
	StreamOpts
	Channel <-chan interface{}
}

func NewChannelStreamOpts(namespace, name string, channel <-chan interface{}) *ChannelStreamOpts {
	return &ChannelStreamOpts{
		StreamOpts: StreamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace, "mixin",
				MIXIN_STREAM_CHANNEL, "name", name),
			MetricGet: noopMetric,
		},
		Channel: channel,
	}
}

// ChannelStream forms a stream from a channel.
type ChannelStream struct {
	streamFields
	channel <-chan interface{}
}

// Create a ChannelStream that gets Messages from $channel.
func NewChannelStream(opts ChannelStreamOpts) *ChannelStream {
	return &ChannelStream{
		streamFields: *newStreamFields(&opts.StreamOpts),
		channel:      opts.Channel,
	}
}

func (r *ChannelStream) Get() (Message, error) {
	begin := time.Now()
	r.log.Debug("[Stream] Get() ...")
	switch v := (<-r.channel).(type) {
	case Message:
		timespan := time.Now().Sub(begin).Seconds()
		r.log.Debug("[Stream] Get() ok", "msg_out", v.Id(),
			"timespan", timespan)
		r.mxGet.Observe(timespan, label_ok)
		return v, nil
	case error:
		timespan := time.Now().Sub(begin).Seconds()
		r.log.Debug("[Stream] Get() err", "err", v,
			"timespan", timespan)
		r.mxGet.Observe(timespan, label_err)
		return nil, v
	default:
		timespan := time.Now().Sub(begin).Seconds()
		r.log.Error("[Stream] Get() err, invalid type",
			"value", v, "timespan", timespan)
		return nil, ErrInvalidType
	}
}

func (r *ChannelStream) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin:     MIXIN_STREAM_CHANNEL,
		Name:      r.name,
	}
}

type HandlerMappedStreamOpts struct {
	StreamOpts
}

func NewHandlerMappedStreamOpts(namespace, name string) *HandlerMappedStreamOpts {
	return &HandlerMappedStreamOpts{
		StreamOpts: StreamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace, "mixin",
				MIXIN_STREAM_HANDLED, "name", name),
			MetricGet: noopMetric,
		},
	}
}

// A HandlerMappedStream whose Get() emits a Message transformed by a Handler from
// a given Stream.
type HandlerMappedStream struct {
	streamFields
	upstream Stream
	handler  Handler
}

func NewHandlerMappedStream(opts HandlerMappedStreamOpts, upstream Stream, handler Handler) *HandlerMappedStream {
	return &HandlerMappedStream{
		streamFields: *newStreamFields(&opts.StreamOpts),
		upstream:     upstream,
		handler:      handler,
	}
}

func (r *HandlerMappedStream) Get() (Message, error) {
	begin := time.Now()
	r.log.Debug("[Stream] upstream.Get() ...")
	msg, err := r.upstream.Get()
	if err != nil {
		r.log.Error("[Stream] upstream.Get() err", "err", err)
		r.mxGet.Observe(time.Now().Sub(begin).Seconds(), label_err)
		return nil, err
	}
	r.log.Debug("[Stream] upstream.Get() ok, Handle() ...", "msg", msg.Id())
	hmsg, herr := r.handler.Handle(msg)
	timespan := time.Now().Sub(begin).Seconds()
	if herr != nil {
		r.log.Error("[Stream] Handle() err", "msg", msg.Id(), "err", herr,
			"timespan", timespan)
		r.mxGet.Observe(timespan, label_err)
		return nil, herr
	}
	r.log.Debug("[Stream] Handle() ok", "msg_in", msg.Id(),
		"msg_out", hmsg.Id(), "timespan", timespan)
	r.mxGet.Observe(timespan, label_ok)
	return hmsg, nil
}

func (r *HandlerMappedStream) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin:     MIXIN_STREAM_HANDLED,
		Name:      r.name,
	}
}

type simpleStream struct {
	getFunc func() (Message, error)
}

func (r *simpleStream) Get() (Message, error) {
	return r.getFunc()
}

// A helper to create a simplest Stream without facilities like logger and
// metrics.
func NewSimpleStream(getFunc func() (Message, error)) Stream {
	return &simpleStream{
		getFunc: getFunc,
	}
}
