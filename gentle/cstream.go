package gentle

import (
	"context"
	"github.com/benbjohnson/clock"
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
	r.log.For(ctx).Info("[Stream] Get() ...")

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
