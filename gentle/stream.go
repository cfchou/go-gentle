package gentle

import (
	"context"
	"errors"
	"github.com/benbjohnson/clock"
	"github.com/cfchou/hystrix-go/hystrix"
	"github.com/opentracing/opentracing-go"
	"time"
)

// SimpleStream turns a function into a Stream
type SimpleStream func(context.Context) (Message, error)

func (r SimpleStream) Get(ctx context.Context) (Message, error) {
	return r(ctx)
}

// Common options for XxxxStreamOpts
type streamOpts struct {
	Namespace  string
	Name       string
	Log        Logger
	Tracer     opentracing.Tracer
	TracingRef TracingRef
}

// Common fields for XxxxStream
type streamFields struct {
	namespace  string
	name       string
	log        loggerFactory
	tracer     opentracing.Tracer
	tracingRef TracingRef
}

func newStreamFields(opts *streamOpts) *streamFields {
	return &streamFields{
		namespace:  opts.Namespace,
		name:       opts.Name,
		log:        loggerFactory{opts.Log},
		tracer:     opts.Tracer,
		tracingRef: opts.TracingRef,
	}
}

type RateLimitedStreamOpts struct {
	streamOpts
	Metric  Metric
	Limiter RateLimit
}

func NewRateLimitedStreamOpts(namespace, name string, limiter RateLimit) *RateLimitedStreamOpts {
	return &RateLimitedStreamOpts{
		streamOpts: streamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"gentle", StreamRateLimited, "name", name),
			Tracer:     opentracing.GlobalTracer(),
			TracingRef: TracingChildOf,
		},
		Metric:  noopMetric,
		Limiter: limiter,
	}
}

// Rate limiting pattern is used to limit the speed of a series of Get().
type rateLimitedStream struct {
	*streamFields
	metric  Metric
	limiter RateLimit
	stream  Stream
}

func NewRateLimitedStream(opts *RateLimitedStreamOpts, upstream Stream) Stream {
	return &rateLimitedStream{
		streamFields: newStreamFields(&opts.streamOpts),
		metric:       opts.Metric,
		limiter:      opts.Limiter,
		stream:       upstream,
	}
}

// Get() is blocked when the limit is reached.
func (r *rateLimitedStream) Get(ctx context.Context) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	if err == nil {
		r.log.For(ctx).Info("[Stream] Get() ...")
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	} else {
		r.log.Bg().Info("[Stream] Get(), no span", "err", err)
	}
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
		timespan := time.Since(begin)
		err := ctx.Err()
		r.log.For(ctx).Warn("[Stream] Wait() interrupted", "err", err,
			"timespan", timespan.Seconds())
		r.metric.ObserveErr(timespan)
		return nil, err
	case <-c:
	}
	// NOTE:
	// We don't simultaneously check ctx.Done() because it's down to
	// stream.Get() to respect timeout/cancellation and to release resource
	// acquired. This behaviour aligns thread-join model.
	msg, err := r.stream.Get(ctx)
	timespan := time.Since(begin)
	if err != nil {
		r.log.For(ctx).Error("[Stream] Get() err", "err", err,
			"timespan", timespan.Seconds())
		r.metric.ObserveErr(timespan)
		return nil, err
	}
	r.log.For(ctx).Debug("[Stream] Get() ok", "msgOut", msg.ID(),
		"timespan", timespan.Seconds())
	r.metric.ObserveOk(timespan)
	return msg, nil
}

type RetryStreamOpts struct {
	streamOpts
	RetryMetric RetryMetric
	// TODO
	// remove the dependency to package clock for this exported symbol
	Clock          clock.Clock
	BackOffFactory BackOffFactory
}

func NewRetryStreamOpts(namespace, name string, backOffFactory BackOffFactory) *RetryStreamOpts {
	return &RetryStreamOpts{
		streamOpts: streamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace, "gentle",
				StreamRetry, "name", name),
			Tracer:     opentracing.GlobalTracer(),
			TracingRef: TracingChildOf,
		},
		RetryMetric:    noopRetryMetric,
		Clock:          clock.New(),
		BackOffFactory: backOffFactory,
	}
}

// retryStream will, when Get() encounters error, back off for some time
// and then retries.
type retryStream struct {
	*streamFields
	retryMetric    RetryMetric
	clock          clock.Clock
	backOffFactory BackOffFactory
	stream         Stream
}

func NewRetryStream(opts *RetryStreamOpts, upstream Stream) Stream {
	return &retryStream{
		streamFields:   newStreamFields(&opts.streamOpts),
		retryMetric:    opts.RetryMetric,
		clock:          opts.Clock,
		backOffFactory: opts.BackOffFactory,
		stream:         upstream,
	}
}

func (r *retryStream) Get(ctx context.Context) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	if err == nil {
		r.log.For(ctx).Info("[Stream] Get() ...")
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	} else {
		r.log.Bg().Info("[Stream] Get(), no span", "err", err)
	}
	begin := r.clock.Now()

	returnOk := func(info string, msg Message, retry int) (Message, error) {
		timespan := r.clock.Now().Sub(begin)
		r.log.For(ctx).Debug(info, "msgOut", msg.ID(),
			"timespan", timespan.Seconds(), "retry", retry)
		r.retryMetric.ObserveOk(timespan, retry)
		return msg, nil
	}
	returnNotOk := func(lvl, info string, err error, retry int) (Message, error) {
		timespan := r.clock.Now().Sub(begin)
		if lvl == "warn" {
			r.log.For(ctx).Warn(info, "err", err,
				"timespan", timespan.Seconds(), "retry", retry)
		} else {
			r.log.For(ctx).Error(info, "err", err,
				"timespan", timespan.Seconds(), "retry", retry)
		}
		r.retryMetric.ObserveErr(timespan, retry)
		return nil, err
	}
	returnErr := func(info string, err error, retry int) (Message, error) {
		return returnNotOk("error", info, err, retry)
	}
	returnWarn := func(info string, err error, retry int) (Message, error) {
		return returnNotOk("warn", info, err, retry)
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
		return returnWarn("[Stream] NewBackOff() interrupted", ctx.Err(), retry)
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
			// Cancellation happens likely during stream.Handle(). We choose to
			// report ctx.Err() instead of err
			return returnWarn("[Stream] Get() interrupted", ctx.Err(), retry)
		}
		// In case BackOff.Next() takes too much time
		c := make(chan time.Duration, 1)
		go func() {
			c <- backOff.Next()
		}()
		var toWait time.Duration
		select {
		case <-ctx.Done():
			return returnWarn("[Stream] Next() interrupted", ctx.Err(), retry)
		case toWait = <-c:
			if toWait == BackOffStop {
				return returnErr("[Stream] Get() err and BackOffStop", err, retry)
			}
		}
		r.log.For(ctx).Debug("[Stream] Get() err, backing off ...",
			"err", err, "elapsed", r.clock.Now().Sub(begin).Seconds(), "retry", retry,
			"backoff", toWait.Seconds())
		tm := r.clock.Timer(toWait)
		select {
		case <-ctx.Done():
			tm.Stop()
			return returnWarn("[Stream] wait interrupted", ctx.Err(), retry)
		case <-tm.C:
		}
		retry++
	}
}

type BulkheadStreamOpts struct {
	streamOpts
	Metric         Metric
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
			Tracer:     opentracing.GlobalTracer(),
			TracingRef: TracingChildOf,
		},
		Metric:         noopMetric,
		MaxConcurrency: maxConcurrency,
	}
}

// Bulkhead pattern is used to limit the number of concurrently hanging Get().
// It uses semaphore isolation, similar to the approach used in hystrix.
// http://stackoverflow.com/questions/30391809/what-is-bulkhead-pattern-used-by-hystrix
type bulkheadStream struct {
	*streamFields
	metric    Metric
	stream    Stream
	semaphore chan struct{}
}

// Create a bulkheadStream that allows at maximum $max_concurrency Get() to
// run concurrently.
func NewBulkheadStream(opts *BulkheadStreamOpts, upstream Stream) Stream {
	return &bulkheadStream{
		streamFields: newStreamFields(&opts.streamOpts),
		metric:       opts.Metric,
		stream:       upstream,
		semaphore:    make(chan struct{}, opts.MaxConcurrency),
	}
}

// Get() returns ErrMaxConcurrency when passing the threshold.
func (r *bulkheadStream) Get(ctx context.Context) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	if err == nil {
		r.log.For(ctx).Info("[Stream] Get() ...")
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	} else {
		r.log.Bg().Info("[Stream] Get(), no span", "err", err)
	}
	begin := time.Now()

	select {
	case r.semaphore <- struct{}{}:
		defer func() {
			<-r.semaphore
		}()
		msg, err := r.stream.Get(ctx)
		timespan := time.Since(begin)
		if err != nil {
			r.log.For(ctx).Error("[Stream] Get() err", "err", err,
				"timespan", timespan.Seconds())
			r.metric.ObserveErr(timespan)
			return nil, err
		}
		r.log.For(ctx).Debug("[Stream] Get() ok", "msgOut", msg.ID(),
			"timespan", timespan.Seconds())
		r.metric.ObserveOk(timespan)
		return msg, nil
	default:
		timespan := time.Since(begin)
		r.log.For(ctx).Error("[Stream] Get() err", "err", ErrMaxConcurrency,
			"timespan", timespan.Seconds())
		r.metric.ObserveErr(timespan)
		return nil, ErrMaxConcurrency
	}
}

func (r *bulkheadStream) GetMaxConcurrency() int {
	return cap(r.semaphore)
}

func (r *bulkheadStream) GetCurrentConcurrency() int {
	return len(r.semaphore)
}

type CircuitStreamOpts struct {
	streamOpts
	CbMetric CbMetric
	Circuit  string
}

func NewCircuitStreamOpts(namespace, name, circuit string) *CircuitStreamOpts {
	return &CircuitStreamOpts{
		streamOpts: streamOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"gentle", StreamCircuit,
				"name", name, "circuit", circuit),
			Tracer:     opentracing.GlobalTracer(),
			TracingRef: TracingChildOf,
		},
		CbMetric: noopCbMetric,
		Circuit:  circuit,
	}
}

// circuitStream is a Stream equipped with a circuit-breaker.
type circuitStream struct {
	*streamFields
	cbMetric CbMetric
	circuit  string
	stream   Stream
}

// In hystrix-go, a circuit-breaker must be given a unique name.
// NewCircuitStream() creates a circuitStream with a
// circuit-breaker named $circuit.
func NewCircuitStream(opts *CircuitStreamOpts, stream Stream) Stream {

	// Note that if it might overwrite or be overwritten by concurrently
	// registering the same circuit.
	allCircuits := hystrix.GetCircuitSettings()
	if _, ok := allCircuits[opts.Circuit]; !ok {
		NewDefaultCircuitConf().RegisterFor(opts.Circuit)
	}

	return &circuitStream{
		streamFields: newStreamFields(&opts.streamOpts),
		cbMetric:     opts.CbMetric,
		circuit:      opts.Circuit,
		stream:       stream,
	}
}

func (r *circuitStream) Get(ctx context.Context) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	if err == nil {
		r.log.For(ctx).Info("[Stream] Get() ...")
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	} else {
		r.log.Bg().Info("[Stream] Get(), no span", "err", err)
	}
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
		timespan := time.Since(begin)
		// To prevent misinterpreting when wrapping one circuitStream
		// over another. Hystrix errors are replaced so that Get() won't return
		// any hystrix errors.
		switch err {
		case hystrix.ErrCircuitOpen:
			err = ErrCbOpen
			r.log.For(ctx).Error("[Stream] Circuit err", "err", err,
				"timespan", timespan.Seconds())
		case hystrix.ErrMaxConcurrency:
			err = ErrCbMaxConcurrency
			r.log.For(ctx).Error("[Stream] Circuit err", "err", err,
				"timespan", timespan.Seconds())
		case hystrix.ErrTimeout:
			err = ErrCbTimeout
			r.log.For(ctx).Error("[Stream] Circuit err", "err", err,
				"timespan", timespan.Seconds())
		default:
			// Captured error from stream.Get()
		}
		r.cbMetric.ObserveErr(timespan, err)
		return nil, err
	}
	msgOut := (<-result).(Message)
	timespan := time.Since(begin)
	r.log.For(ctx).Debug("[Stream] Get() ok", "msgOut", msgOut.ID(),
		"timespan", timespan.Seconds())
	r.cbMetric.ObserveOk(timespan)
	return msgOut, nil
}

func (r *circuitStream) GetCircuitName() string {
	return r.circuit
}
