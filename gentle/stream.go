package gentle

import (
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/benbjohnson/clock"
	"sync"
	"time"
)

const (
	// Types of resilience, are most often used as part of RegistryKey.
	StreamRateLimited    = "sRate"
	StreamRetry          = "sRetry"
	StreamBulkhead       = "sBulk"
	StreamSemaphore      = "sSem"
	StreamCircuitBreaker = "sCircuit"
	StreamChannel        = "sChan"
	//StreamHandled        = "sHan"
	//StreamFallback       = "sFb"
)

var (
	labelOk  = map[string]string{"result": "ok"}
	labelErr = map[string]string{"result": "err"}
)

// Common options for XXXStreamOpts
type streamOpts struct {
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

func newStreamFields(opts *streamOpts) *streamFields {
	return &streamFields{
		namespace: opts.Namespace,
		name:      opts.Name,
		log:       opts.Log,
		mxGet:     opts.MetricGet,
	}
}

type RateLimitedStreamOpts struct {
	streamOpts
	Limiter RateLimit
}

func NewRateLimitedStreamOpts(namespace, name string, limiter RateLimit) *RateLimitedStreamOpts {
	return &RateLimitedStreamOpts{
		streamOpts: streamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"gentle", StreamRateLimited, "name", name),
			MetricGet: noopMetric,
		},
		Limiter: limiter,
	}
}

// Rate limiting pattern is used to limit the speed of a series of Get().
type rateLimitedStream struct {
	*streamFields
	limiter RateLimit
	stream  Stream
}

func NewRateLimitedStream(opts *RateLimitedStreamOpts, upstream Stream) Stream {
	return &rateLimitedStream{
		streamFields: newStreamFields(&opts.streamOpts),
		limiter:      opts.Limiter,
		stream:       upstream,
	}
}

// Get() is blocked when the limit is exceeded.
func (r *rateLimitedStream) Get() (Message, error) {
	begin := time.Now()
	r.log.Debug("[Stream] Get() ...")
	r.limiter.Wait(1, 0)
	msg, err := r.stream.Get()
	timespan := time.Since(begin).Seconds()
	if err != nil {
		r.log.Error("[Stream] Get() err", "err", err,
			"timespan", timespan)
		r.mxGet.Observe(timespan, labelErr)
		return nil, err
	}
	r.log.Debug("[Stream] Get() ok", "msgOut", msg.ID(),
		"timespan", timespan)
	r.mxGet.Observe(timespan, labelOk)
	return msg, nil
}

func (r *rateLimitedStream) GetNames() *Names {
	return &Names{
		Namespace:  r.namespace,
		Resilience: StreamRateLimited,
		Name:       r.name,
	}
}

type RetryStreamOpts struct {
	streamOpts
	MetricTryNum   Metric
	Clock          Clock
	BackOffFactory BackOffFactory
}

func NewRetryStreamOpts(namespace, name string, backOffFactory BackOffFactory) *RetryStreamOpts {
	return &RetryStreamOpts{
		streamOpts: streamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace, "gentle",
				StreamRetry, "name", name),
			MetricGet: noopMetric,
		},
		MetricTryNum:   noopMetric,
		Clock:          clock.New(),
		BackOffFactory: backOffFactory,
	}
}

// retryStream will, when Get() encounters error, back off for some time
// and then retries.
type retryStream struct {
	*streamFields
	obTryNum       Metric
	clock          Clock
	backOffFactory BackOffFactory
	stream         Stream
}

func NewRetryStream(opts *RetryStreamOpts, upstream Stream) Stream {
	return &retryStream{
		streamFields:   newStreamFields(&opts.streamOpts),
		obTryNum:       opts.MetricTryNum,
		clock:          opts.Clock,
		backOffFactory: opts.BackOffFactory,
		stream:         upstream,
	}
}

func (r *retryStream) Get() (Message, error) {
	begin := r.clock.Now()
	count := 1
	r.log.Debug("[Stream] Get() ...", "count", count)
	var once sync.Once
	var backOff BackOff
	for {
		msg, err := r.stream.Get()
		if err == nil {
			timespan := r.clock.Now().Sub(begin).Seconds()
			r.log.Debug("[Stream] Get() ok", "msgOut", msg.ID(),
				"timespan", timespan, "count", count)
			r.mxGet.Observe(timespan, labelOk)
			r.obTryNum.Observe(float64(count), labelOk)
			return msg, nil
		}
		once.Do(func() {
			backOff = r.backOffFactory.NewBackOff()
		})
		toWait := backOff.Next()
		// Next() should immediately return but we can't guarantee so
		// timespan is calculated after Next().
		timespan := r.clock.Now().Sub(begin).Seconds()
		if toWait == BackOffStop {
			r.log.Error("[Streamer] Get() err and no more backing off",
				"err", err, "timespan", timespan,
				"count", count)
			r.mxGet.Observe(timespan, labelErr)
			r.obTryNum.Observe(float64(count), labelErr)
			return nil, err
		}
		// timespan in our convention is used to track the overall
		// time of current function. Here we record time
		// passed as "elapsed".
		count++
		r.log.Error("[Stream] Get() err, backing off ...",
			"err", err, "elapsed", timespan, "count", count,
			"wait", toWait)
		r.clock.Sleep(toWait)
	}
}

func (r *retryStream) GetNames() *Names {
	return &Names{
		Namespace:  r.namespace,
		Resilience: StreamRetry,
		Name:       r.name,
	}
}

type BulkheadStreamOpts struct {
	streamOpts
	MaxConcurrency int
}

func NewBulkheadStreamOpts(namespace, name string, maxConcurrency int) *BulkheadStreamOpts {
	if maxConcurrency <= 0 {
		panic(errors.New("max_concurrent must be greater than 0"))
	}

	return &BulkheadStreamOpts{
		streamOpts: streamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace, "gentle",
				StreamBulkhead, "name", name),
			MetricGet: noopMetric,
		},
		MaxConcurrency: maxConcurrency,
	}
}

// Bulkhead pattern is used to limit the number of concurrently hanging Get().
// It uses semaphore isolation, similar to the approach used in hystrix.
// http://stackoverflow.com/questions/30391809/what-is-bulkhead-pattern-used-by-hystrix
type bulkheadStream struct {
	*streamFields
	stream    Stream
	semaphore chan struct{}
}

// Create a bulkheadStream that allows at maximum $max_concurrency Get() to
// run concurrently.
func NewBulkheadStream(opts *BulkheadStreamOpts, upstream Stream) Stream {

	return &bulkheadStream{
		streamFields: newStreamFields(&opts.streamOpts),
		stream:       upstream,
		semaphore:    make(chan struct{}, opts.MaxConcurrency),
	}
}

// Get() is blocked when the limit is exceeded.
func (r *bulkheadStream) Get() (Message, error) {
	begin := time.Now()
	r.log.Debug("[Stream] Get() ...")
	select {
	case r.semaphore <- struct{}{}:
		defer func() {
			<-r.semaphore
		}()
		msg, err := r.stream.Get()
		timespan := time.Since(begin).Seconds()
		if err != nil {
			r.log.Error("[Stream] Get() err", "err", err,
				"timespan", timespan)
			r.mxGet.Observe(timespan, labelErr)
			return nil, err
		}
		r.log.Debug("[Stream] Get() ok", "msgOut", msg.ID(),
			"timespan", timespan)
		r.mxGet.Observe(timespan, labelOk)
		return msg, nil
	default:
		r.log.Error("[Stream] Get() err", "err", ErrMaxConcurrency)
		return nil, ErrMaxConcurrency
	}
}

func (r *bulkheadStream) GetNames() *Names {
	return &Names{
		Namespace:  r.namespace,
		Resilience: StreamBulkhead,
		Name:       r.name,
	}
}

func (r *bulkheadStream) GetMaxConcurrency() int {
	return cap(r.semaphore)
}

func (r *bulkheadStream) GetCurrentConcurrency() int {
	return len(r.semaphore)
}

type SemaphoreStreamOpts struct {
	streamOpts
	MaxConcurrency int
}

func NewSemaphoreStreamOpts(namespace, name string, maxConcurrency int) *SemaphoreStreamOpts {
	if maxConcurrency <= 0 {
		panic(errors.New("max_concurrent must be greater than 0"))
	}

	return &SemaphoreStreamOpts{
		streamOpts: streamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace, "gentle",
				StreamSemaphore, "name", name),
			MetricGet: noopMetric,
		},
		MaxConcurrency: maxConcurrency,
	}
}

// It allows at maximum $max_concurrency Get() to run concurrently. Similar
// to Bulkhead, but it blocks when MaxConcurrency is reached.
type semaphoreStream struct {
	*streamFields
	stream    Stream
	semaphore chan struct{}
}

func NewSemaphoreStream(opts *SemaphoreStreamOpts, upstream Stream) Stream {

	return &semaphoreStream{
		streamFields: newStreamFields(&opts.streamOpts),
		stream:       upstream,
		semaphore:    make(chan struct{}, opts.MaxConcurrency),
	}
}

func (r *semaphoreStream) Get() (Message, error) {
	begin := time.Now()
	r.log.Debug("[Stream] Get() ...")
	r.semaphore <- struct{}{}
	defer func() { <-r.semaphore }()
	msg, err := r.stream.Get()
	timespan := time.Since(begin).Seconds()
	if err != nil {
		r.log.Error("[Stream] Get() err", "err", err,
			"timespan", timespan)
		r.mxGet.Observe(timespan, labelErr)
		return nil, err
	}
	r.log.Debug("[Stream] Get() ok", "msgOut", msg.ID(),
		"timespan", timespan)
	r.mxGet.Observe(timespan, labelOk)
	return msg, nil
}

func (r *semaphoreStream) GetNames() *Names {
	return &Names{
		Namespace:  r.namespace,
		Resilience: StreamSemaphore,
		Name:       r.name,
	}
}

func (r *semaphoreStream) GetMaxConcurrency() int {
	return cap(r.semaphore)
}

func (r *semaphoreStream) GetCurrentConcurrency() int {
	return len(r.semaphore)
}

type CircuitBreakerStreamOpts struct {
	streamOpts
	MetricCbErr Metric
	Circuit     string
}

func NewCircuitBreakerStreamOpts(namespace, name, circuit string) *CircuitBreakerStreamOpts {
	return &CircuitBreakerStreamOpts{
		streamOpts: streamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"gentle", StreamCircuitBreaker,
				"name", name, "circuit", circuit),
			MetricGet: noopMetric,
		},
		MetricCbErr: noopMetric,
		Circuit:     circuit,
	}
}

// circuitBreakerStream is a Stream equipped with a circuit-breaker.
type circuitBreakerStream struct {
	*streamFields
	mxCbErr Metric
	circuit string
	stream  Stream
}

// In hystrix-go, a circuit-breaker must be given a unique name.
// NewCircuitBreakerStream() creates a circuitBreakerStream with a
// circuit-breaker named $circuit.
func NewCircuitBreakerStream(opts *CircuitBreakerStreamOpts, stream Stream) Stream {

	// Note that if it might overwrite or be overwritten by concurrently
	// registering the same circuit.
	allCircuits := hystrix.GetCircuitSettings()
	if _, ok := allCircuits[opts.Circuit]; !ok {
		NewDefaultCircuitBreakerConf().RegisterFor(opts.Circuit)
	}

	return &circuitBreakerStream{
		streamFields: newStreamFields(&opts.streamOpts),
		mxCbErr:      opts.MetricCbErr,
		circuit:      opts.Circuit,
		stream:       stream,
	}
}

func (r *circuitBreakerStream) Get() (Message, error) {
	begin := time.Now()
	r.log.Debug("[Stream] Get() ...")
	result := make(chan interface{}, 1)
	err := hystrix.Do(r.circuit, func() error {
		msg, err := r.stream.Get()
		timespan := time.Since(begin).Seconds()
		if err != nil {
			r.log.Error("[Stream] Do()::Get() err",
				"err", err, "timespan", timespan)
			return err
		}
		r.log.Debug("[Stream] Do()::Get() ok",
			"msgOut", msg.ID(), "timespan", timespan)
		result <- msg
		return nil
	}, nil)
	// NOTE:
	// err can be from Do()::Get() or hystrix errors if criteria are matched.
	// Do()::Get()'s err, being returned or not, contributes to hystrix metrics
	if err != nil {
		defer func() {
			timespan := time.Since(begin).Seconds()
			r.log.Error("[Stream] Circuit err", "err", err,
				"timespan", timespan)
			r.mxGet.Observe(timespan, labelErr)
		}()
		// To prevent misinterpreting when wrapping one
		// circuitBreakerStream over another. Hystrix errors are
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
	msgOut := (<-result).(Message)
	timespan := time.Since(begin).Seconds()
	r.log.Debug("[Stream] Get() ok", "msgOut", msgOut.ID(),
		"timespan", timespan)
	r.mxGet.Observe(timespan, labelOk)
	return msgOut, nil
}

func (r *circuitBreakerStream) GetNames() *Names {
	return &Names{
		Namespace:  r.namespace,
		Resilience: StreamCircuitBreaker,
		Name:       r.name,
	}
}

func (r *circuitBreakerStream) GetCircuitName() string {
	return r.circuit
}

/*
type FallbackStreamOpts struct {
	streamOpts
	FallbackFunc func(error) (Message, error)
}

func NewFallbackStreamOpts(namespace, name string,
	fallbackFunc func(error) (Message, error)) *FallbackStreamOpts {
	return &FallbackStreamOpts{
		streamOpts: streamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"gentle", StreamFallback, "name", name),
			MetricGet: noopMetric,
		},
		FallbackFunc: fallbackFunc,
	}
}

// fallbackStream transforms what Stream.Get() returns.
type fallbackStream struct {
	*streamFields
	fallbackFunc func(error) (Message, error)
	stream       Stream
}

func NewFallbackStream(opts *FallbackStreamOpts, upstream Stream) Stream {
	return &fallbackStream{
		streamFields: newStreamFields(&opts.streamOpts),
		fallbackFunc: opts.FallbackFunc,
		stream:       upstream,
	}
}

func (r *fallbackStream) Get() (Message, error) {
	begin := time.Now()
	msg, err := r.stream.Get()
	if err == nil {
		timespan := time.Since(begin).Seconds()
		r.log.Debug("[Stream] Get() ok, skip fallbackFunc",
			"msg", msg.ID(), "timespan", timespan)
		r.mxGet.Observe(timespan, labelOk)
		return msg, nil
	}
	r.log.Debug("[Stream] Get() err, fallbackFunc() ...", "err", err)
	// fallback to deal with the err
	msg, err = r.fallbackFunc(err)
	timespan := time.Since(begin).Seconds()
	if err != nil {
		r.log.Error("[Stream] fallbackFunc() err",
			"err", err, "timespan", timespan)
		r.mxGet.Observe(timespan, labelErr)
		return nil, err
	}
	r.log.Debug("[Stream] fallbackFunc() ok",
		"msg", msg.ID(), "timespan", timespan)
	r.mxGet.Observe(timespan, labelOk)
	return msg, nil
}

func (r *fallbackStream) GetNames() *Names {
	return &Names{
		Namespace:  r.namespace,
		Resilience: StreamFallback,
		Name:       r.name,
	}
}
*/

type ChannelStreamOpts struct {
	streamOpts
	Channel <-chan interface{}
}

func NewChannelStreamOpts(namespace, name string, channel <-chan interface{}) *ChannelStreamOpts {
	return &ChannelStreamOpts{
		streamOpts: streamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace, "gentle",
				StreamChannel, "name", name),
			MetricGet: noopMetric,
		},
		Channel: channel,
	}
}

// channelStream forms a stream from a channel.
type channelStream struct {
	*streamFields
	channel <-chan interface{}
}

// Create a channelStream that gets Messages from $channel.
func NewChannelStream(opts *ChannelStreamOpts) Stream {
	return &channelStream{
		streamFields: newStreamFields(&opts.streamOpts),
		channel:      opts.Channel,
	}
}

func (r *channelStream) Get() (Message, error) {
	begin := time.Now()
	r.log.Debug("[Stream] Get() ...")
	switch v := (<-r.channel).(type) {
	case Message:
		timespan := time.Since(begin).Seconds()
		r.log.Debug("[Stream] Get() ok", "msgOut", v.ID(),
			"timespan", timespan)
		r.mxGet.Observe(timespan, labelOk)
		return v, nil
	case error:
		timespan := time.Since(begin).Seconds()
		r.log.Debug("[Stream] Get() err", "err", v,
			"timespan", timespan)
		r.mxGet.Observe(timespan, labelErr)
		return nil, v
	default:
		timespan := time.Since(begin).Seconds()
		r.log.Error("[Stream] Get() err, invalid type",
			"value", v, "timespan", timespan)
		return nil, ErrInvalidType
	}
}

func (r *channelStream) GetNames() *Names {
	return &Names{
		Namespace:  r.namespace,
		Resilience: StreamChannel,
		Name:       r.name,
	}
}

