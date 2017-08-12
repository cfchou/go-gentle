package gentle

import (
	"context"
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
		r.log.For(ctx).Error("[Handler] Handle() err", "msgIn", msg.ID(),
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
