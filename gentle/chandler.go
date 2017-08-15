package gentle

import (
	"context"
	"errors"
	"github.com/benbjohnson/clock"
	"github.com/opentracing/opentracing-go"
	"time"
)

// Common options for XXXHandlerOpts
type chandlerOpts struct {
	Namespace    string
	Name         string
	Log          Logger
	Tracer       opentracing.Tracer
	TracingRef   TracingRef
	MetricHandle Metric
}

// Common fields for XXXHandler
type chandlerFields struct {
	namespace  string
	name       string
	log        loggerFactory
	tracer     opentracing.Tracer
	tracingRef TracingRef
	mxHandle   Metric
}

func newCHandlerFields(opts *chandlerOpts) *chandlerFields {
	return &chandlerFields{
		namespace:  opts.Namespace,
		name:       opts.Name,
		log:        loggerFactory{opts.Log},
		tracer:     opts.Tracer,
		tracingRef: opts.TracingRef,
		mxHandle:   opts.MetricHandle,
	}
}

type RateLimitedCHandlerOpts struct {
	chandlerOpts
	Limiter RateLimit
}

func NewRateLimitedCHandlerOpts(namespace, name string, limiter RateLimit) *RateLimitedCHandlerOpts {
	return &RateLimitedCHandlerOpts{
		chandlerOpts: chandlerOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"gentle", HandlerRateLimited, "name", name),
			Tracer:       opentracing.GlobalTracer(),
			TracingRef:   TracingChildOf,
			MetricHandle: noopMetric,
		},
		Limiter: limiter,
	}
}

// Rate limiting pattern is used to limit the speed of a series of Handle().
type rateLimitedCHandler struct {
	*chandlerFields
	limiter RateLimit
	handler CHandler
}

func NewRateLimitedCHandler(opts *RateLimitedCHandlerOpts, handler CHandler) CHandler {
	return &rateLimitedCHandler{
		chandlerFields: newCHandlerFields(&opts.chandlerOpts),
		limiter:        opts.Limiter,
		handler:        handler,
	}
}

// Handle() is blocked when the limit is exceeded.
func (r *rateLimitedCHandler) Handle(ctx context.Context, msg Message) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	if err == nil {
		r.log.Bg().Debug("[Stream] New span err", "err", err)
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	}

	begin := time.Now()
	r.log.For(ctx).Debug("[Handler] Handle() ...", "msgIn", msg.ID())

	c := make(chan struct{}, 1)
	go func() {
		// FIXME
		// see rateLimitedStream
		r.limiter.Wait(1, 0)
		c <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		timespan := time.Since(begin).Seconds()
		err := ctx.Err()
		r.log.For(ctx).Warn("[Handler] Handle() err", "msgIn", msg.ID(),
			"err", err, "timespan", timespan)
		r.mxHandle.Observe(timespan, labelErr)
		return nil, err
	case <-c:
	}
	msgOut, err := r.handler.Handle(ctx, msg)
	timespan := time.Since(begin).Seconds()
	if err != nil {
		r.log.For(ctx).Error("[Handler] Handle() err", "msgIn", msg.ID(),
			"err", err, "timespan", timespan)
		r.mxHandle.Observe(timespan, labelErr)
		return nil, err
	}
	r.log.For(ctx).Debug("[Handler] Handle() ok", "msgIn", msg.ID(),
		"msgOut", msgOut.ID(), "timespan", timespan)
	r.mxHandle.Observe(timespan, labelOk)
	return msgOut, nil
}

func (r *rateLimitedCHandler) GetNames() *Names {
	return &Names{
		Namespace:  r.namespace,
		Resilience: HandlerRateLimited,
		Name:       r.name,
	}
}

type RetryCHandlerOpts struct {
	chandlerOpts
	MetricTryNum Metric
	// TODO
	// remove the dependency to package clock for this exported symbol
	Clock          clock.Clock
	BackOffFactory BackOffFactory
}

func NewRetryCHandlerOpts(namespace, name string, backOffFactory BackOffFactory) *RetryCHandlerOpts {
	return &RetryCHandlerOpts{
		chandlerOpts: chandlerOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace, "gentle",
				HandlerRetry, "name", name),
			Tracer:       opentracing.GlobalTracer(),
			TracingRef:   TracingChildOf,
			MetricHandle: noopMetric,
		},
		MetricTryNum:   noopMetric,
		Clock:          clock.New(),
		BackOffFactory: backOffFactory,
	}
}

// retryHandler takes an Handler. When Handler.Handle() encounters an error,
// retryHandler back off for some time and then retries.
type retryCHandler struct {
	*chandlerFields
	mxTryNum       Metric
	clock          clock.Clock
	backOffFactory BackOffFactory
	handler        CHandler
}

func NewRetryCHandler(opts *RetryCHandlerOpts, handler CHandler) CHandler {
	return &retryCHandler{
		chandlerFields: newCHandlerFields(&opts.chandlerOpts),
		mxTryNum:       opts.MetricTryNum,
		clock:          opts.Clock,
		backOffFactory: opts.BackOffFactory,
		handler:        handler,
	}
}

func (r *retryCHandler) Handle(ctx context.Context, msg Message) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	if err == nil {
		r.log.Bg().Debug("[Handler] New span err", "err", err)
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	}
	r.log.For(ctx).Info("[Handler] Handle() ...")
	begin := r.clock.Now()

	returnOk := func(info string, msgIn, msgOut Message, retry int) (Message, error) {
		timespan := r.clock.Now().Sub(begin).Seconds()
		r.log.For(ctx).Debug(info, "msgIn", msgIn.ID(), "msgOut", msgOut.ID(),
			"timespan", timespan, "retry", retry)
		r.mxHandle.Observe(timespan, labelOk)
		r.mxTryNum.Observe(float64(retry), labelOk)
		return msg, nil
	}
	returnNotOk := func(lvl, info string, msgIn Message, err error, retry int) (Message, error) {
		timespan := r.clock.Now().Sub(begin).Seconds()
		if lvl == "warn" {
			r.log.For(ctx).Warn(info, "msgIn", msgIn.ID(), "err", err,
				"timespan", timespan, "retry", retry)
		} else {
			r.log.For(ctx).Error(info, "msgIn", msgIn.ID(), "err", err,
				"timespan", timespan, "retry", retry)
		}
		r.mxHandle.Observe(timespan, labelErr)
		r.mxTryNum.Observe(float64(retry), labelErr)
		return nil, err
	}
	returnErr := func(info string, msgIn Message, err error, retry int) (Message, error) {
		return returnNotOk("error", info, msgIn, err, retry)
	}
	returnWarn := func(info string, msgIn Message, err error, retry int) (Message, error) {
		return returnNotOk("warn", info, msgIn, err, retry)
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
		return returnWarn("[Handler] NewBackOff() interrupted", msg, ctx.Err(), retry)
	case backOff = <-c:
	}
	for {
		msgOut, err := r.handler.Handle(ctx, msg)
		if err == nil {
			// If it's interrupt at this point, we choose to return successfully.
			return returnOk("[Handler] Handle() ok", msg, msgOut, retry)
		}
		if ctx.Err() != nil {
			// This check is an optimization in that it still could be captured
			// in the latter select.
			// Cancellation doesn't necessarily happen during Get() but it's
			// likely. We choose to report ctx.Err() instead of err
			return returnWarn("[Handler] Handle() interrupted", msg, ctx.Err(), retry)
		}
		// In case BackOff.Next() takes too much time
		c := make(chan time.Duration, 1)
		go func() {
			c <- backOff.Next()
		}()
		var toWait time.Duration
		select {
		case <-ctx.Done():
			return returnWarn("[Handler] Next() interrupted", msg, ctx.Err(), retry)
		case toWait = <-c:
			if toWait == BackOffStop {
				return returnErr("[Handler] Handle() err and BackOffStop", msg, err, retry)
			}
		}
		r.log.For(ctx).Debug("[Handler] Get() err, backing off ...",
			"err", err, "elapsed", r.clock.Now().Sub(begin).Seconds(), "retry", retry,
			"backoff", toWait.Seconds())
		tm := r.clock.Timer(toWait)
		select {
		case <-ctx.Done():
			tm.Stop()
			return returnWarn("[Streamer] wait interrupted", msg, ctx.Err(), retry)
		case <-tm.C:
		}
		retry++
	}
}

func (r *retryCHandler) GetNames() *Names {
	return &Names{
		Namespace:  r.namespace,
		Resilience: HandlerRetry,
		Name:       r.name,
	}
}

type BulkheadCHandlerOpts struct {
	chandlerOpts
	MaxConcurrency int
}

func NewBulkheadCHandlerOpts(namespace, name string, maxConcurrency int) *BulkheadCHandlerOpts {
	if maxConcurrency <= 0 {
		panic(errors.New("maxConcurrent must be greater than 0"))
	}
	return &BulkheadCHandlerOpts{
		chandlerOpts: chandlerOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"gentle", HandlerBulkhead, "name", name),
			Tracer:       opentracing.GlobalTracer(),
			TracingRef:   TracingChildOf,
			MetricHandle: noopMetric,
		},
		MaxConcurrency: maxConcurrency,
	}
}

type bulkheadCHandler struct {
	*chandlerFields
	handler   CHandler
	semaphore chan struct{}
}

// Create a bulkheadHandler that allows at maximum $max_concurrency Handle() to
// run concurrently.
func NewBulkheadCHandler(opts *BulkheadCHandlerOpts, handler CHandler) CHandler {

	return &bulkheadCHandler{
		chandlerFields: newCHandlerFields(&opts.chandlerOpts),
		handler:        handler,
		semaphore:      make(chan struct{}, opts.MaxConcurrency),
	}
}

// Handle() returns ErrMaxConcurrency when passing the threshold.
func (r *bulkheadCHandler) Handle(ctx context.Context, msg Message) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	if err == nil {
		r.log.Bg().Debug("[Handle] New span err", "err", err)
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	}
	r.log.For(ctx).Info("[Handle] Get() ...")
	begin := time.Now()

	select {
	case r.semaphore <- struct{}{}:
		defer func() {
			<-r.semaphore
		}()
		msgOut, err := r.handler.Handle(ctx, msg)
		timespan := time.Since(begin).Seconds()
		if err != nil {
			r.log.For(ctx).Error("[Handler] Handle() err", "msgIn", msg.ID(),
				"err", err, "timespan", timespan)
			r.mxHandle.Observe(timespan, labelErr)
			return nil, err
		}
		r.log.For(ctx).Debug("[Handler] Handle() ok", "msgIn", msg.ID(),
			"msgOut", msgOut.ID(), "timespan", timespan)
		r.mxHandle.Observe(timespan, labelOk)
		return msgOut, nil
	default:
		r.log.For(ctx).Error("[Hander] Handle() err", "msgIn", msg.ID(),
			"err", ErrMaxConcurrency)
		return nil, ErrMaxConcurrency
	}
}

func (r *bulkheadCHandler) GetNames() *Names {
	return &Names{
		Namespace:  r.namespace,
		Resilience: HandlerBulkhead,
		Name:       r.name,
	}
}

func (r *bulkheadCHandler) GetMaxConcurrency() int {
	return cap(r.semaphore)
}

func (r *bulkheadCHandler) GetCurrentConcurrency() int {
	return len(r.semaphore)
}
