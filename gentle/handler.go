package gentle

import (
	"context"
	"errors"
	"github.com/benbjohnson/clock"
	"github.com/cfchou/hystrix-go/hystrix"
	"github.com/opentracing/opentracing-go"
	"time"
)

// SimpleHandler turns a function into a Handler
type SimpleHandler func(context.Context, Message) (Message, error)

func (r SimpleHandler) Handle(ctx context.Context, msg Message) (Message, error) {
	return r(ctx, msg)
}

// Common options for XXXHandlerOpts
type handlerOpts struct {
	Namespace  string
	Name       string
	Log        Logger
	Tracer     opentracing.Tracer
	TracingRef TracingRef
}

// Common fields for XXXHandler
type handlerFields struct {
	namespace  string
	name       string
	log        loggerFactory
	tracer     opentracing.Tracer
	tracingRef TracingRef
}

func newHandlerFields(opts *handlerOpts) *handlerFields {
	return &handlerFields{
		namespace:  opts.Namespace,
		name:       opts.Name,
		log:        loggerFactory{opts.Log},
		tracer:     opts.Tracer,
		tracingRef: opts.TracingRef,
	}
}

type RateLimitedHandlerOpts struct {
	handlerOpts
	Metric  Metric
	Limiter RateLimit
}

func NewRateLimitedHandlerOpts(namespace, name string, limiter RateLimit) *RateLimitedHandlerOpts {
	return &RateLimitedHandlerOpts{
		handlerOpts: handlerOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"gentle", HandlerRateLimited, "name", name),
			Tracer:     opentracing.GlobalTracer(),
			TracingRef: TracingChildOf,
		},
		Metric:  noopMetric,
		Limiter: limiter,
	}
}

// Rate limiting pattern is used to limit the speed of a series of Handle().
type rateLimitedHandler struct {
	*handlerFields
	metric  Metric
	limiter RateLimit
	handler Handler
}

func NewRateLimitedHandler(opts *RateLimitedHandlerOpts, handler Handler) Handler {
	return &rateLimitedHandler{
		handlerFields: newHandlerFields(&opts.handlerOpts),
		metric:        opts.Metric,
		limiter:       opts.Limiter,
		handler:       handler,
	}
}

// Handle() is blocked when the limit is reached.
func (r *rateLimitedHandler) Handle(ctx context.Context, msg Message) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	if err == nil {
		r.log.For(ctx).Info("[Handler] Handle() ...", "msgIn", msg.ID())
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	} else {
		r.log.Bg().Debug("[Handler] New span err", "msgIn", msg.ID(),
			"err", err)
	}
	begin := time.Now()

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
		r.metric.ObserveErr(timespan)
		return nil, err
	case <-c:
	}
	msgOut, err := r.handler.Handle(ctx, msg)
	timespan := time.Since(begin).Seconds()
	if err != nil {
		r.log.For(ctx).Error("[Handler] Handle() err", "msgIn", msg.ID(),
			"err", err, "timespan", timespan)
		r.metric.ObserveErr(timespan)
		return nil, err
	}
	r.log.For(ctx).Debug("[Handler] Handle() ok", "msgIn", msg.ID(),
		"msgOut", msgOut.ID(), "timespan", timespan)
	r.metric.ObserveOk(timespan)
	return msgOut, nil
}

type RetryHandlerOpts struct {
	handlerOpts
	RetryMetric RetryMetric
	// TODO
	// remove the dependency to package clock for this exported symbol
	Clock          clock.Clock
	BackOffFactory BackOffFactory
}

func NewRetryHandlerOpts(namespace, name string, backOffFactory BackOffFactory) *RetryHandlerOpts {
	return &RetryHandlerOpts{
		handlerOpts: handlerOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace, "gentle",
				HandlerRetry, "name", name),
			Tracer:     opentracing.GlobalTracer(),
			TracingRef: TracingChildOf,
		},
		RetryMetric:    noopRetryMetric,
		Clock:          clock.New(),
		BackOffFactory: backOffFactory,
	}
}

// retryHandler takes an Handler. When Handler.Handle() encounters an error,
// retryHandler back off for some time and then retries.
type retryHandler struct {
	*handlerFields
	retryMetric    RetryMetric
	clock          clock.Clock
	backOffFactory BackOffFactory
	handler        Handler
}

func NewRetryHandler(opts *RetryHandlerOpts, handler Handler) Handler {
	return &retryHandler{
		handlerFields:  newHandlerFields(&opts.handlerOpts),
		retryMetric:    opts.RetryMetric,
		clock:          opts.Clock,
		backOffFactory: opts.BackOffFactory,
		handler:        handler,
	}
}

func (r *retryHandler) Handle(ctx context.Context, msg Message) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	if err == nil {
		r.log.For(ctx).Info("[Handler] Handle() ...", "msgIn", msg.ID())
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	} else {
		r.log.Bg().Debug("[Handler] New span err", "msgIn", msg.ID(),
			"err", err)
	}
	begin := r.clock.Now()

	returnOk := func(info string, msgIn, msgOut Message, retry int) (Message, error) {
		timespan := r.clock.Now().Sub(begin).Seconds()
		r.log.For(ctx).Debug(info, "msgIn", msgIn.ID(), "msgOut", msgOut.ID(),
			"timespan", timespan, "retry", retry)
		r.retryMetric.ObserveOk(timespan, retry)
		return msgOut, nil
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
		r.retryMetric.ObserveErr(timespan, retry)
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
			// Cancellation happens likely during handler.Handle(). We choose to
			// report ctx.Err() instead of err
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
		r.log.For(ctx).Debug("[Handler] Handle() err, backing off ...",
			"err", err, "elapsed", r.clock.Now().Sub(begin).Seconds(), "retry", retry,
			"backoff", toWait.Seconds())
		tm := r.clock.Timer(toWait)
		select {
		case <-ctx.Done():
			tm.Stop()
			return returnWarn("[Handler] wait interrupted", msg, ctx.Err(), retry)
		case <-tm.C:
		}
		retry++
	}
}

type BulkheadHandlerOpts struct {
	handlerOpts
	Metric         Metric
	MaxConcurrency int
}

func NewBulkheadHandlerOpts(namespace, name string, maxConcurrency int) *BulkheadHandlerOpts {
	if maxConcurrency <= 0 {
		panic(errors.New("maxConcurrent must be greater than 0"))
	}
	return &BulkheadHandlerOpts{
		handlerOpts: handlerOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"gentle", HandlerBulkhead, "name", name),
			Tracer:     opentracing.GlobalTracer(),
			TracingRef: TracingChildOf,
		},
		Metric:         noopMetric,
		MaxConcurrency: maxConcurrency,
	}
}

type bulkheadHandler struct {
	*handlerFields
	metric    Metric
	handler   Handler
	semaphore chan struct{}
}

// Create a bulkheadHandler that allows at maximum $max_concurrency Handle() to
// run concurrently.
func NewBulkheadHandler(opts *BulkheadHandlerOpts, handler Handler) Handler {

	return &bulkheadHandler{
		handlerFields: newHandlerFields(&opts.handlerOpts),
		metric:        opts.Metric,
		handler:       handler,
		semaphore:     make(chan struct{}, opts.MaxConcurrency),
	}
}

// Handle() returns ErrMaxConcurrency when passing the threshold.
func (r *bulkheadHandler) Handle(ctx context.Context, msg Message) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	if err == nil {
		r.log.For(ctx).Info("[Handler] Handle() ...", "msgIn", msg.ID())
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	} else {
		r.log.Bg().Debug("[Handler] New span err", "msgIn", msg.ID(),
			"err", err)
	}
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
			r.metric.ObserveErr(timespan)
			return nil, err
		}
		r.log.For(ctx).Debug("[Handler] Handle() ok", "msgIn", msg.ID(),
			"msgOut", msgOut.ID(), "timespan", timespan)
		r.metric.ObserveOk(timespan)
		return msgOut, nil
	default:
		timespan := time.Since(begin).Seconds()
		r.log.For(ctx).Error("[Hander] Handle() err", "msgIn", msg.ID(),
			"err", ErrMaxConcurrency, "timespan", timespan)
		r.metric.ObserveErr(timespan)
		return nil, ErrMaxConcurrency
	}
}

func (r *bulkheadHandler) GetMaxConcurrency() int {
	return cap(r.semaphore)
}

func (r *bulkheadHandler) GetCurrentConcurrency() int {
	return len(r.semaphore)
}

type CircuitHandlerOpts struct {
	handlerOpts
	CbMetric CbMetric
	Circuit  string
}

func NewCircuitHandlerOpts(namespace, name, circuit string) *CircuitHandlerOpts {
	return &CircuitHandlerOpts{
		handlerOpts: handlerOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"gentle", HandlerCircuit,
				"name", name, "circuit", circuit),
			Tracer:     opentracing.GlobalTracer(),
			TracingRef: TracingChildOf,
		},
		CbMetric: noopCbMetric,
		Circuit:  circuit,
	}
}

type circuitHandler struct {
	*handlerFields
	cbMetric CbMetric
	circuit  string
	handler  Handler
}

// In hystrix-go, a circuit-breaker must be given a unique name.
// NewCircuitStream() creates a circuitStream with a
// circuit-breaker named $circuit.
func NewCircuitHandler(opts *CircuitHandlerOpts, handler Handler) Handler {
	return &circuitHandler{
		handlerFields: newHandlerFields(&opts.handlerOpts),
		cbMetric:      opts.CbMetric,
		circuit:       opts.Circuit,
		handler:       handler,
	}
}

func (r *circuitHandler) Handle(ctx context.Context, msg Message) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	if err == nil {
		r.log.For(ctx).Info("[Handler] Handle() ...", "msgIn", msg.ID())
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	} else {
		r.log.Bg().Debug("[Handler] New span err", "msgIn", msg.ID(),
			"err", err)
	}
	begin := time.Now()

	result := make(chan interface{}, 1)
	err = hystrix.Do(r.circuit, func() error {
		msgOut, err := r.handler.Handle(ctx, msg)
		timespan := time.Since(begin).Seconds()
		if err != nil {
			r.log.For(ctx).Error("[Handler] handler.Handle() err",
				"msgIn", msg.ID(), "err", err, "timespan", timespan)
			// NOTE:
			// 1. This err could be captured outside if a hystrix's error
			//    doesn't take precedence.
			// 2. Being captured or not, it contributes to hystrix metrics.
			return err
		}
		r.log.For(ctx).Debug("[Handler] handller.Handle() ok",
			"msgIn", msg.ID(), "msgOut", msgOut.ID(),
			"timespan", timespan)
		result <- msgOut
		return nil
	}, nil)
	// NOTE:
	// Capturing error from handler.Handle() or from hystrix if criteria met.
	if err != nil {
		timespan := time.Since(begin).Seconds()
		// To prevent misinterpreting when wrapping one circuitStream
		// over another. Hystrix errors are replaced so that Get() won't return
		// any hystrix errors.
		switch err {
		case hystrix.ErrCircuitOpen:
			err = ErrCbOpen
			r.log.For(ctx).Error("[Handler] Circuit err",
				"msgIn", msg.ID(), "err", err, "timespan", timespan)
		case hystrix.ErrMaxConcurrency:
			err = ErrCbMaxConcurrency
			r.log.For(ctx).Error("[Handler] Circuit err",
				"msgIn", msg.ID(), "err", err, "timespan", timespan)
		case hystrix.ErrTimeout:
			err = ErrCbTimeout
			r.log.For(ctx).Error("[Handler] Circuit err",
				"msgIn", msg.ID(), "err", err, "timespan", timespan)
		default:
			// Captured error from handler.Get()
		}
		r.cbMetric.ObserveErr(timespan, err)
		return nil, err
	}
	msgOut := (<-result).(Message)
	timespan := time.Since(begin).Seconds()
	r.log.For(ctx).Debug("[Handler] Handle() ok", "msgIn", msg.ID(),
		"msgOut", msgOut.ID(), "timespan", timespan)
	r.cbMetric.ObserveOk(timespan)
	return msgOut, nil
}

func (r *circuitHandler) GetCircuitName() string {
	return r.circuit
}
