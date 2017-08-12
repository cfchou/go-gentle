package gentle

import (
	"context"
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

	begin := time.Now()
	r.log.For(ctx).Debug("[Stream] Get() ...")

	c := make(chan struct{}, 1)
	go func() {
		// FIXME
		// A possible timer-leakage is created when ctx.Done() is triggered but
		// the underlying timer is still running until a bucket available. To
		// fix it, RateLimit.Wait() might as well need to support context.
		r.limiter.Wait(1, 0)
		c <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		timespan := time.Since(begin).Seconds()
		err := ctx.Err()
		r.log.For(ctx).Error("[Stream] Get() err", "err", err,
			"timespan", timespan)
		r.mxGet.Observe(timespan, labelErr)
		return nil, err
	case <-c:
	}
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
