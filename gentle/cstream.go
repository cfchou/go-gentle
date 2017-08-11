package gentle

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"time"
)

type privateInterface interface {
	private()
}

type Lol interface {
	Debug(msg string, ctx ...interface{})
}

type logger struct {
	logger Lol
}

func (l logger) Debug(msg string, ctx ...interface{}) {
	l.logger.Debug(msg, ctx)
}

func (l logger) private() {}

type spanLogger struct {
	logger Lol
	span   opentracing.Span
}

func (l spanLogger) Debug(msg string, ctx ...interface{}) {
	l.span.LogFields(
		log.String("level", "debug"),
		log.String("message", msg),
	)
	l.logger.Debug(msg, ctx)
}

func (l spanLogger) private() {}

// loggerFactory is the default logging wrapper that can create
// logger instances either for a given Context or context-less.
type loggerFactory struct {
	logger Lol
}

// NewloggerFactory creates a new loggerFactory.
//
// FIXME:
// It's possible NewloggerFactory(NewloggerFactory(lg)):
//
//   func oops(ctx context.Context, lg Lol) loggerFactory {
//   	return NewloggerFactory(NewloggerFactory(lg).For(ctx))
//   }
func NewloggerFactory(logger Lol) loggerFactory {
	if _, ok := logger.(privateInterface); ok {
		panic("Nested logger is not allowed")
	}
	return loggerFactory{logger: logger}
}

func (b loggerFactory) Bg() Lol {
	return logger{logger: b.logger}
}

func (b loggerFactory) For(ctx context.Context) Lol {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		return spanLogger{logger: b.logger}
	}
	return b.Bg()
}

/*

// Bg creates a context-unaware logger.
func (b loggerFactory) Bg() Logger {
	return b.logger
}

// For returns a context-aware Logger. If the context
// contains an OpenTracing span, all logging calls are also
// echo-ed into the span.
func (b loggerFactory) For(ctx context.Context) Logger {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		// TODO for Jaeger span extract trace/span IDs as fields
		return spanLogger{span: span, logger: b.logger}
	}
	return b.Bg()
}
*/

// Common fields for XXXStream
type cstreamFields struct {
	namespace string
	name      string
	log       Logger
	mxGet     Metric
}

// Rate limiting pattern is used to limit the speed of a series of Get().
type cRateLimitedStream struct {
	*streamFields
	limiter RateLimit
	stream  CStream
}

func NewRateLimitedCStream(opts *RateLimitedStreamOpts, upstream CStream) CStream {
	return &cRateLimitedStream{
		streamFields: newStreamFields(&opts.streamOpts),
		limiter:      opts.Limiter,
		stream:       upstream,
	}
}

// Get() is blocked when the limit is exceeded.
func (r *cRateLimitedStream) Get(ctx context.Context) (Message, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, StreamRateLimited)
	defer span.Finish()

	// TODO:
	// If opentracing is used, timespan and msgOut in log may not be needed

	begin := time.Now()
	r.log.Debug("[Stream] Get() ...")

	c := make(chan struct{}, 1)
	go func() {
		// FIXME
		// When ctx.Done() is triggered, the underlying timer is still running
		// until a bucket available. It creates a timer-leakage.
		r.limiter.Wait(1, 0)
		c <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		timespan := time.Since(begin).Seconds()
		err := ctx.Err()
		r.log.Error("[Stream] Get() err", "err", err,
			"timespan", timespan)
		r.mxGet.Observe(timespan, labelErr)
		return nil, err
	case <-c:
	}
	msg, err := r.stream.Get(ctx)
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
