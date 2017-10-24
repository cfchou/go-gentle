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

// Get emits a message.
func (r SimpleStream) Get(ctx context.Context) (Message, error) {
	return r(ctx)
}

// StreamOpts is options that every XxxStreamOpts must have.
type StreamOpts struct {
	Namespace  string
	Name       string
	Log        Logger
	Tracer     opentracing.Tracer
	TracingRef TracingRef
}

// Common fields for XxxStream
type streamFields struct {
	namespace  string
	name       string
	log        loggerFactory
	tracer     opentracing.Tracer
	tracingRef TracingRef
}

func newStreamFields(opts *StreamOpts) *streamFields {
	return &streamFields{
		namespace:  opts.Namespace,
		name:       opts.Name,
		log:        loggerFactory{opts.Log},
		tracer:     opts.Tracer,
		tracingRef: opts.TracingRef,
	}
}

// RateLimitedStreamOpts contains options that'll be used by NewRateLimitedStream.
type RateLimitedStreamOpts struct {
	StreamOpts
	Metric  Metric
	Limiter RateLimit
}

// NewRateLimitedStreamOpts returns RateLimitedStreamOpts with default values.
func NewRateLimitedStreamOpts(namespace, name string, limiter RateLimit) *RateLimitedStreamOpts {
	return &RateLimitedStreamOpts{
		StreamOpts: StreamOpts{
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

// RateLimitedStream is a Stream that runs the upstream in a rate-limited manner.
type RateLimitedStream struct {
	*streamFields
	metric  Metric
	limiter RateLimit
	stream  Stream
}

// NewRateLimitedStream creates a RateLimitedStream to guard the upstream.
func NewRateLimitedStream(opts *RateLimitedStreamOpts, upstream Stream) *RateLimitedStream {
	return &RateLimitedStream{
		streamFields: newStreamFields(&opts.StreamOpts),
		metric:       opts.Metric,
		limiter:      opts.Limiter,
		stream:       upstream,
	}
}

// Get blocks when requests coming too fast.
func (r *RateLimitedStream) Get(ctx context.Context) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	var log Logger
	if err == nil {
		log = r.log.For(ctx)
		log.Info("Get() ...")
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	} else {
		log = r.log.Bg()
		log.Info("Get() w/o span ...")
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
		log.Warn("Wait() interrupted", "err", err,
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
		log.Error("Get() err", "err", err,
			"timespan", timespan.Seconds())
		r.metric.ObserveErr(timespan)
		return nil, err
	}
	log.Debug("Get() ok", "msgOut", msg.ID(),
		"timespan", timespan.Seconds())
	r.metric.ObserveOk(timespan)
	return msg, nil
}

// RetryStreamOpts contains options that'll be used by NewRetryStream.
type RetryStreamOpts struct {
	StreamOpts
	RetryMetric RetryMetric
	// TODO
	// remove the dependency to package clock for this exported symbol
	Clock          clock.Clock
	BackOffFactory BackOffFactory
}

// NewRetryStreamOpts returns RetryStreamOpts with default values.
func NewRetryStreamOpts(namespace, name string, backOffFactory BackOffFactory) *RetryStreamOpts {
	return &RetryStreamOpts{
		StreamOpts: StreamOpts{
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

// RetryStream will, when Get() encounters error, back off for some time
// and then retries.
type RetryStream struct {
	*streamFields
	retryMetric    RetryMetric
	clock          clock.Clock
	backOffFactory BackOffFactory
	stream         Stream
}

// NewRetryStream creates a RetryStream to guard the upstream.
func NewRetryStream(opts *RetryStreamOpts, upstream Stream) *RetryStream {
	return &RetryStream{
		streamFields:   newStreamFields(&opts.StreamOpts),
		retryMetric:    opts.RetryMetric,
		clock:          opts.Clock,
		backOffFactory: opts.BackOffFactory,
		stream:         upstream,
	}
}

// Get retries with back-offs when upstream returns an error.
func (r *RetryStream) Get(ctx context.Context) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	var log Logger
	if err == nil {
		log = r.log.For(ctx)
		log.Info("Get() ...")
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	} else {
		log = r.log.Bg()
		log.Info("Get() w/o span ...")
	}
	begin := r.clock.Now()

	returnOk := func(info string, msg Message, retry int) (Message, error) {
		timespan := r.clock.Now().Sub(begin)
		log.Debug(info, "msgOut", msg.ID(),
			"timespan", timespan.Seconds(), "retry", retry)
		r.retryMetric.ObserveOk(timespan, retry)
		return msg, nil
	}
	returnNotOk := func(lvl, info string, err error, retry int) (Message, error) {
		timespan := r.clock.Now().Sub(begin)
		if lvl == "warn" {
			log.Warn(info, "err", err,
				"timespan", timespan.Seconds(), "retry", retry)
		} else {
			log.Error(info, "err", err,
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
		return returnWarn("NewBackOff() interrupted", ctx.Err(), retry)
	case backOff = <-c:
	}
	for {
		msg, err := r.stream.Get(ctx)
		if err == nil {
			// If it's interrupt at this point, we choose to return successfully.
			return returnOk("Get() ok", msg, retry)
		}
		if ctx.Err() != nil {
			// This check is an optimization in that it still could be captured
			// in the latter select.
			// Cancellation happens likely during stream.Handle(). We choose to
			// report ctx.Err() instead of err
			return returnWarn("Get() interrupted", ctx.Err(), retry)
		}
		// In case BackOff.Next() takes too much time
		c := make(chan time.Duration, 1)
		go func() {
			c <- backOff.Next()
		}()
		var toWait time.Duration
		select {
		case <-ctx.Done():
			return returnWarn("Next() interrupted", ctx.Err(), retry)
		case toWait = <-c:
			if toWait == BackOffStop {
				return returnErr("Get() err and BackOffStop", err, retry)
			}
		}
		log.Debug("Get() err, backing off ...",
			"err", err, "elapsed", r.clock.Now().Sub(begin).Seconds(), "retry", retry,
			"backoff", toWait.Seconds())
		tm := r.clock.Timer(toWait)
		select {
		case <-ctx.Done():
			tm.Stop()
			return returnWarn("wait interrupted", ctx.Err(), retry)
		case <-tm.C:
		}
		retry++
	}
}

// BulkheadStreamOpts contains options that'll be used by NewBulkheadStream.
type BulkheadStreamOpts struct {
	StreamOpts
	Metric         Metric
	MaxConcurrency int
}

// NewBulkheadStreamOpts returns BulkheadStreamOpts with default values.
func NewBulkheadStreamOpts(namespace, name string, maxConcurrency int) *BulkheadStreamOpts {
	if maxConcurrency <= 0 {
		panic(errors.New("max_concurrent must be greater than 0"))
	}

	return &BulkheadStreamOpts{
		StreamOpts: StreamOpts{
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

// BulkheadStream is a Stream that limits concurrent access to the upstream.
type BulkheadStream struct {
	*streamFields
	metric    Metric
	stream    Stream
	semaphore chan struct{}
}

// NewBulkheadStream creates a BulkheadStream to guard the upstream.
func NewBulkheadStream(opts *BulkheadStreamOpts, upstream Stream) *BulkheadStream {
	return &BulkheadStream{
		streamFields: newStreamFields(&opts.StreamOpts),
		metric:       opts.Metric,
		stream:       upstream,
		semaphore:    make(chan struct{}, opts.MaxConcurrency),
	}
}

// Get returns ErrMaxConcurrency when running over the threshold.
func (r *BulkheadStream) Get(ctx context.Context) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	var log Logger
	if err == nil {
		log = r.log.For(ctx)
		log.Info("Get() ...")
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	} else {
		log = r.log.Bg()
		log.Info("Get() w/o span ...")
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
			log.Error("Get() err", "err", err,
				"timespan", timespan.Seconds())
			r.metric.ObserveErr(timespan)
			return nil, err
		}
		log.Debug("Get() ok", "msgOut", msg.ID(),
			"timespan", timespan.Seconds())
		r.metric.ObserveOk(timespan)
		return msg, nil
	default:
		timespan := time.Since(begin)
		log.Error("Get() err", "err", ErrMaxConcurrency,
			"timespan", timespan.Seconds())
		r.metric.ObserveErr(timespan)
		return nil, ErrMaxConcurrency
	}
}

// CircuitStreamOpts contains options that'll be used by NewCircuitStream.
type CircuitStreamOpts struct {
	StreamOpts
	CbMetric CbMetric

	// Circuit is the name of the circuit-breaker to create. Each circuit-breaker
	// must have an unique name associated to its CircuitConf and internal hystrix metrics.
	Circuit     string
	CircuitConf CircuitConf
}

// NewCircuitStreamOpts returns CircuitStreamOpts with default values.
// Note that circuit is the name of the circuit. Every circuit must have a unique
// name that maps to its CircuitConf.
func NewCircuitStreamOpts(namespace, name, circuit string) *CircuitStreamOpts {
	return &CircuitStreamOpts{
		StreamOpts: StreamOpts{
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
		CircuitConf: CircuitConf{
			Timeout:               DefaultCbTimeout,
			MaxConcurrent:         DefaultCbMaxConcurrent,
			VolumeThreshold:       DefaultCbVolumeThreshold,
			ErrorPercentThreshold: DefaultCbErrPercentThreshold,
			SleepWindow:           DefaultCbSleepWindow,
		},
	}
}

// CircuitStream is a Stream that guards the upstream with a circuit-breaker.
type CircuitStream struct {
	*streamFields
	cbMetric CbMetric
	circuit  string
	stream   Stream
}

// NewCircuitStream creates a CircuitStream to guard the upstream.
func NewCircuitStream(opts *CircuitStreamOpts, upstream Stream) *CircuitStream {
	// Note that if it might overwrite or be overwritten existing setting
	hystrix.ConfigureCommand(opts.Circuit, hystrix.CommandConfig{
		Timeout:                int(opts.CircuitConf.Timeout / time.Millisecond),
		MaxConcurrentRequests:  opts.CircuitConf.MaxConcurrent,
		RequestVolumeThreshold: opts.CircuitConf.VolumeThreshold,
		SleepWindow:            int(opts.CircuitConf.SleepWindow / time.Millisecond),
		ErrorPercentThreshold:  opts.CircuitConf.ErrorPercentThreshold,
	})
	return &CircuitStream{
		streamFields: newStreamFields(&opts.StreamOpts),
		cbMetric:     opts.CbMetric,
		circuit:      opts.Circuit,
		stream:       upstream,
	}
}

// Get may return errors that generated by the upstream and errors from circuit
// itself including ErrCircuitOpen, ErrCbMaxConcurrency and ErrCbTimeout.
func (r *CircuitStream) Get(ctx context.Context) (Message, error) {
	ctx, err := contextWithNewSpan(ctx, r.tracer, r.tracingRef)
	var log Logger
	if err == nil {
		log = r.log.For(ctx)
		log.Info("Get() ...")
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	} else {
		log = r.log.Bg()
		log.Info("Get() w/o span ...")
	}
	begin := time.Now()

	result := make(chan interface{}, 1)
	err = hystrix.Do(r.circuit, func() error {
		msg, err := r.stream.Get(ctx)
		timespan := time.Since(begin).Seconds()
		if err != nil {
			log.Error("stream.Get() err",
				"err", err, "timespan", timespan)
			// NOTE:
			// 1. This err could be captured outside if a hystrix's error
			//    doesn't take precedence.
			// 2. Being captured or not, it contributes to hystrix metrics.
			return err
		}
		log.Debug("stream.Get() ok",
			"msgOut", msg.ID(), "timespan", timespan)
		result <- msg
		return nil
	}, nil)
	// NOTE:
	// Capturing error from stream.Get() or from hystrix if criteria met.
	if err != nil {
		timespan := time.Since(begin)
		// To prevent misinterpreting when wrapping one CircuitStream
		// over another. Hystrix errors are replaced so that Get() won't return
		// any hystrix errors.
		switch err {
		case hystrix.ErrCircuitOpen:
			err = ErrCbOpen
			log.Error("Circuit err", "err", err,
				"timespan", timespan.Seconds())
		case hystrix.ErrMaxConcurrency:
			err = ErrCbMaxConcurrency
			log.Error("Circuit err", "err", err,
				"timespan", timespan.Seconds())
		case hystrix.ErrTimeout:
			err = ErrCbTimeout
			log.Error("Circuit err", "err", err,
				"timespan", timespan.Seconds())
		default:
			// Captured error from stream.Get()
		}
		r.cbMetric.ObserveErr(timespan, err)
		return nil, err
	}
	msgOut := (<-result).(Message)
	timespan := time.Since(begin)
	log.Debug("Get() ok", "msgOut", msgOut.ID(),
		"timespan", timespan.Seconds())
	r.cbMetric.ObserveOk(timespan)
	return msgOut, nil
}

// GetCircuitName returns the name of the internal circuit.
func (r *CircuitStream) GetCircuitName() string {
	return r.circuit
}
