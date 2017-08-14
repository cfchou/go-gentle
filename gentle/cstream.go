package gentle

import (
	"context"
	"errors"
	"github.com/benbjohnson/clock"
	"github.com/cfchou/hystrix-go/hystrix"
	"github.com/opentracing/opentracing-go"
	"time"
)

// Common options for XXXStreamOpts
type cstreamOpts struct {
	Namespace  string
	Name       string
	Log        Logger
	Tracer     opentracing.Tracer
	TracingRef TracingRef
	MetricGet  Metric
}

// Common fields for XXXStream
type cstreamFields struct {
	namespace  string
	name       string
	log        loggerFactory
	tracer     opentracing.Tracer
	tracingRef TracingRef
	mxGet      Metric
}

func newCStreamFields(opts *cstreamOpts) *cstreamFields {
	return &cstreamFields{
		namespace:  opts.Namespace,
		name:       opts.Name,
		log:        loggerFactory{opts.Log},
		tracer:     opts.Tracer,
		tracingRef: opts.TracingRef,
		mxGet:      opts.MetricGet,
	}
}

type RateLimitedCStreamOpts struct {
	cstreamOpts
	Limiter RateLimit
}

func NewRateLimitedCStreamOpts(namespace, name string, limiter RateLimit) *RateLimitedCStreamOpts {
	return &RateLimitedCStreamOpts{
		cstreamOpts: cstreamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"gentle", StreamRateLimited, "name", name),
			Tracer:     opentracing.GlobalTracer(),
			TracingRef: TracingChildOf,
			MetricGet:  noopMetric,
		},
		Limiter: limiter,
	}
}

// Rate limiting pattern is used to limit the speed of a series of Get().
type rateLimitedCStream struct {
	*cstreamFields
	limiter RateLimit
	stream  CStream
}

func NewRateLimitedCStream(opts *RateLimitedCStreamOpts, upstream CStream) CStream {
	return &rateLimitedCStream{
		cstreamFields: newCStreamFields(&opts.cstreamOpts),
		limiter:       opts.Limiter,
		stream:        upstream,
	}
}

// Get() is blocked when the limit is exceeded.
func (r *rateLimitedCStream) Get(ctx context.Context) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	if err == nil {
		r.log.Bg().Debug("[Stream] New span err", "err", err)
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	}
	r.log.For(ctx).Info("[Stream] Get() ...")
	begin := time.Now()

	c := make(chan struct{}, 1)
	go func(ch chan<- struct{}) {
		// FIXME
		// A possible timer-leakage is created when ctx.Done() is triggered but
		// the underlying timer is still running until a bucket available. To
		// fix it, RateLimit.Wait() might as well need to support context.
		r.limiter.Wait(1, 0)
		ch <- struct{}{}
	}(c)
	select {
	case <-ctx.Done():
		timespan := time.Since(begin).Seconds()
		err := ctx.Err()
		r.log.For(ctx).Error("[Stream] Wait() interrupted", "err", err,
			"timespan", timespan)
		r.mxGet.Observe(timespan, labelErr)
		return nil, err
	case <-c:
	}
	// NOTE:
	// We don't simultaneously check ctx.Done() because it's down to
	// stream.Get() to respect timeout/cancellation and to release resource
	// acquired. This behaviour aligns thread-join model.
	msg, err := r.stream.Get(ctx)
	timespan := time.Since(begin).Seconds()
	if err != nil {
		r.log.For(ctx).Error("[Stream] Get() err", "err", err,
			"timespan", timespan)
		r.mxGet.Observe(timespan, labelErr)
		return nil, err
	}
	r.log.For(ctx).Debug("[Stream] Get() ok", "msgOut", msg.ID(),
		"timespan", timespan)
	r.mxGet.Observe(timespan, labelOk)
	return msg, nil
}

func (r *rateLimitedCStream) GetNames() *Names {
	return &Names{
		Namespace:  r.namespace,
		Resilience: StreamRateLimited,
		Name:       r.name,
	}
}

type RetryCStreamOpts struct {
	cstreamOpts
	MetricTryNum Metric
	// TODO
	// remove the dependency to package clock for this exported symbol
	Clock          clock.Clock
	BackOffFactory BackOffFactory
}

func NewRetryCStreamOpts(namespace, name string, backOffFactory BackOffFactory) *RetryCStreamOpts {
	return &RetryCStreamOpts{
		cstreamOpts: cstreamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace, "gentle",
				StreamRetry, "name", name),
			Tracer:     opentracing.GlobalTracer(),
			TracingRef: TracingChildOf,
			MetricGet:  noopMetric,
		},
		MetricTryNum:   noopMetric,
		Clock:          clock.New(),
		BackOffFactory: backOffFactory,
	}
}

// retryStream will, when Get() encounters error, back off for some time
// and then retries.
type retryCStream struct {
	*cstreamFields
	obTryNum       Metric
	clock          clock.Clock
	backOffFactory BackOffFactory
	stream         CStream
}

func NewRetryCStream(opts *RetryCStreamOpts, upstream CStream) CStream {
	return &retryCStream{
		cstreamFields:  newCStreamFields(&opts.cstreamOpts),
		obTryNum:       opts.MetricTryNum,
		clock:          opts.Clock,
		backOffFactory: opts.BackOffFactory,
		stream:         upstream,
	}
}

func (r *retryCStream) Get(ctx context.Context) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	if err == nil {
		r.log.Bg().Debug("[Stream] New span err", "err", err)
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	}
	r.log.For(ctx).Info("[Stream] Get() ...")
	begin := r.clock.Now()

	returnOk := func(info string, msg Message, retry int) (Message, error) {
		timespan := r.clock.Now().Sub(begin).Seconds()
		r.log.For(ctx).Debug(info, "msgOut", msg.ID(), "timespan", timespan,
			"retry", retry)
		r.mxGet.Observe(timespan, labelOk)
		r.obTryNum.Observe(float64(retry), labelOk)
		return msg, nil
	}
	returnErr := func(info string, err error, retry int) (Message, error) {
		timespan := r.clock.Now().Sub(begin).Seconds()
		r.log.For(ctx).Error(info, "err", err, "timespan", timespan,
			"retry", retry)
		r.mxGet.Observe(timespan, labelErr)
		r.obTryNum.Observe(float64(retry), labelErr)
		return nil, err
	}

	retry := 0
	// In case NewBackOff() takes too much time
	c := make(chan BackOff, 1)
	go func() {
		c <- r.backOffFactory.NewBackOff()
	}()
	var backOff BackOff
	select {
	case <-ctx.Done():
		return returnErr("[Streamer] NewBackOff() interrupted", ctx.Err(), retry)
	case backOff = <-c:
	}
	for {
		msg, err := r.stream.Get(ctx)
		if err == nil {
			// If it's interrupt at this point, we choose to return successfully.
			return returnOk("[Stream] Get() ok", msg, retry)
		}
		if ctx.Err() != nil {
			// This check is an optimization in that it still could be captured
			// in the latter select.
			// Cancellation doesn't necessarily happen during Get() but it's
			// likely. We choose to report ctx.Err() instead of err
			return returnErr("[Streamer] Get() interrupted", ctx.Err(), retry)
		}
		// In case BackOff.Next() takes too much time
		c := make(chan time.Duration, 1)
		go func() {
			c <- backOff.Next()
		}()
		var toWait time.Duration
		select {
		case <-ctx.Done():
			return returnErr("[Streamer] Next() interrupted", ctx.Err(), retry)
		case toWait = <-c:
			if toWait == BackOffStop {
				return returnErr("[Streamer] Get() err and no more back-off",
					err, retry)
			}
		}
		r.log.For(ctx).Debug("[Stream] Get() err, backing off ...",
			"err", err, "elapsed", r.clock.Now().Sub(begin).Seconds(), "retry", retry,
			"backoff", toWait)
		tm := r.clock.Timer(toWait)
		select {
		case <-ctx.Done():
			tm.Stop()
			return returnErr("[Streamer] wait interrupted", ctx.Err(), retry)
		case <-tm.C:
		}
		retry++
	}
}

func (r *retryCStream) GetNames() *Names {
	return &Names{
		Namespace:  r.namespace,
		Resilience: StreamRetry,
		Name:       r.name,
	}
}

type BulkheadCStreamOpts struct {
	cstreamOpts
	MaxConcurrency int
}

func NewBulkheadCStreamOpts(namespace, name string, maxConcurrency int) *BulkheadCStreamOpts {
	if maxConcurrency <= 0 {
		panic(errors.New("max_concurrent must be greater than 0"))
	}

	return &BulkheadCStreamOpts{
		cstreamOpts: cstreamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace, "gentle",
				StreamBulkhead, "name", name),
			Tracer:     opentracing.GlobalTracer(),
			TracingRef: TracingChildOf,
			MetricGet:  noopMetric,
		},
		MaxConcurrency: maxConcurrency,
	}
}

// Bulkhead pattern is used to limit the number of concurrently hanging Get().
// It uses semaphore isolation, similar to the approach used in hystrix.
// http://stackoverflow.com/questions/30391809/what-is-bulkhead-pattern-used-by-hystrix
type bulkheadCStream struct {
	*cstreamFields
	stream    CStream
	semaphore chan struct{}
}

// Create a bulkheadStream that allows at maximum $max_concurrency Get() to
// run concurrently.
func NewBulkheadCStream(opts *BulkheadCStreamOpts, upstream CStream) CStream {
	return &bulkheadCStream{
		cstreamFields: newCStreamFields(&opts.cstreamOpts),
		stream:        upstream,
		semaphore:     make(chan struct{}, opts.MaxConcurrency),
	}
}

func (r *bulkheadCStream) Get(ctx context.Context) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	if err == nil {
		r.log.Bg().Debug("[Stream] New span err", "err", err)
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	}
	r.log.For(ctx).Info("[Stream] Get() ...")
	begin := time.Now()

	select {
	case r.semaphore <- struct{}{}:
		defer func() {
			<-r.semaphore
		}()
		msg, err := r.stream.Get(ctx)
		timespan := time.Since(begin).Seconds()
		if err != nil {
			r.log.For(ctx).Error("[Stream] Get() err", "err", err,
				"timespan", timespan)
			r.mxGet.Observe(timespan, labelErr)
			return nil, err
		}
		r.log.For(ctx).Debug("[Stream] Get() ok", "msgOut", msg.ID(),
			"timespan", timespan)
		r.mxGet.Observe(timespan, labelOk)
		return msg, nil
	default:
		r.log.For(ctx).Error("[Stream] Get() err", "err", ErrMaxConcurrency)
		return nil, ErrMaxConcurrency
	}
}

func (r *bulkheadCStream) GetNames() *Names {
	return &Names{
		Namespace:  r.namespace,
		Resilience: StreamBulkhead,
		Name:       r.name,
	}
}

func (r *bulkheadCStream) GetMaxConcurrency() int {
	return cap(r.semaphore)
}

func (r *bulkheadCStream) GetCurrentConcurrency() int {
	return len(r.semaphore)
}

type CircuitBreakerCStreamOpts struct {
	cstreamOpts
	MetricCbErr Metric
	Circuit     string
}

func NewCircuitBreakerCStreamOpts(namespace, name, circuit string) *CircuitBreakerCStreamOpts {
	return &CircuitBreakerCStreamOpts{
		cstreamOpts: cstreamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"gentle", StreamCircuitBreaker,
				"name", name, "circuit", circuit),
			Tracer:     opentracing.GlobalTracer(),
			TracingRef: TracingChildOf,
			MetricGet:  noopMetric,
		},
		MetricCbErr: noopMetric,
		Circuit:     circuit,
	}
}

// circuitBreakerStream is a Stream equipped with a circuit-breaker.
type circuitBreakerCStream struct {
	*cstreamFields
	mxCbErr Metric
	circuit string
	stream  CStream
}

// In hystrix-go, a circuit-breaker must be given a unique name.
// NewCircuitBreakerStream() creates a circuitBreakerStream with a
// circuit-breaker named $circuit.
func NewCircuitBreakerCStream(opts *CircuitBreakerCStreamOpts, stream CStream) CStream {

	// Note that if it might overwrite or be overwritten by concurrently
	// registering the same circuit.
	allCircuits := hystrix.GetCircuitSettings()
	if _, ok := allCircuits[opts.Circuit]; !ok {
		NewDefaultCircuitBreakerConf().RegisterFor(opts.Circuit)
	}

	return &circuitBreakerCStream{
		cstreamFields: newCStreamFields(&opts.cstreamOpts),
		mxCbErr:       opts.MetricCbErr,
		circuit:       opts.Circuit,
		stream:        stream,
	}
}

func (r *circuitBreakerCStream) Get(ctx context.Context) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	if err == nil {
		r.log.Bg().Debug("[Stream] New span err", "err", err)
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	}
	r.log.For(ctx).Info("[Stream] Get() ...")
	begin := time.Now()

	result := make(chan interface{}, 1)
	err = hystrix.Do(r.circuit, func() error {
		msg, err := r.stream.Get(ctx)
		timespan := time.Since(begin).Seconds()
		if err != nil {
			r.log.For(ctx).Error("[Stream] stream.Get() err",
				"err", err, "timespan", timespan)
			// NOTE:
			// 1. This err could be captured outside if a hystrix's error
			//    doesn't take precedence.
			// 2. Being captured or not, it contributes to hystrix metrics.
			return err
		}
		r.log.For(ctx).Debug("[Stream] stream.Get() ok",
			"msgOut", msg.ID(), "timespan", timespan)
		result <- msg
		return nil
	}, nil)
	// NOTE:
	// Capturing error from stream.Get() or from hystrix if criteria met.
	if err != nil {
		timespan := time.Since(begin).Seconds()
		defer func() {
			r.mxGet.Observe(timespan, labelErr)
		}()
		// To prevent misinterpreting when wrapping one circuitBreakerStream
		// over another. Hystrix errors are replaced so that Get() won't return
		// any hystrix errors.
		switch err {
		case hystrix.ErrCircuitOpen:
			r.log.For(ctx).Error("[Stream] Circuit err", "err", err,
				"timespan", timespan)
			r.mxCbErr.Observe(1, map[string]string{"err": "ErrCbOpen"})
			return nil, ErrCbOpen
		case hystrix.ErrMaxConcurrency:
			r.log.For(ctx).Error("[Stream] Circuit err", "err", err,
				"timespan", timespan)
			r.mxCbErr.Observe(1, map[string]string{"err": "ErrCbMaxConcurrency"})
			return nil, ErrCbMaxConcurrency
		case hystrix.ErrTimeout:
			r.log.For(ctx).Error("[Stream] Circuit err", "err", err,
				"timespan", timespan)
			r.mxCbErr.Observe(1, map[string]string{"err": "ErrCbTimeout"})
			return nil, ErrCbTimeout
		default:
			// Captured error from stream::Get()
			r.mxCbErr.Observe(1, map[string]string{"err": "NonCbErr"})
			return nil, err
		}
	}
	msgOut := (<-result).(Message)
	timespan := time.Since(begin).Seconds()
	r.log.For(ctx).Debug("[Stream] Get() ok", "msgOut", msgOut.ID(),
		"timespan", timespan)
	r.mxGet.Observe(timespan, labelOk)
	return msgOut, nil
}

func (r *circuitBreakerCStream) GetNames() *Names {
	return &Names{
		Namespace:  r.namespace,
		Resilience: StreamCircuitBreaker,
		Name:       r.name,
	}
}

func (r *circuitBreakerCStream) GetCircuitName() string {
	return r.circuit
}
