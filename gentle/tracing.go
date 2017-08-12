package gentle

import (
	"context"
	"fmt"
	"github.com/opentracing/opentracing-go"
	tlog "github.com/opentracing/opentracing-go/log"
	"time"
)

// TracingRef is opentracing tracing causal reference, currently either a
// ChildOf or a FlowFrom.
type TracingRef int

const (
	// TracingChildOf represents tracing causal reference ChildOf.
	TracingChildOf TracingRef = iota
	// TracingFlowFrom represents tracing causal reference FromFrom.
	TracingFlowFrom
)

var (
	errNoSpan           = fmt.Errorf("No span")
	errTracingReference = fmt.Errorf("Unsupported tracing reference")
	errNotEvenFields    = fmt.Errorf("Number of log fields is not even")
	errFieldType        = fmt.Errorf("Not valid log field type")
)

// loggerFactory is the default logging wrapper that can create either a
// context-unaware bgLogger or a context-aware spanLogger.
type loggerFactory struct {
	logger Logger
}

// Bg creates a context-unaware logger.
func (b loggerFactory) Bg() Logger {
	return bgLogger{logger: b.logger}
}

// For returns a context-aware Logger. If the context contains an OpenTracing
// span, all logging calls are also echo-ed into the span.
func (b loggerFactory) For(ctx context.Context) Logger {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		return spanLogger{logger: b.logger}
	}
	return b.Bg()
}

type bgLogger struct {
	logger Logger
}

func (l bgLogger) Debug(msg string, fields ...interface{}) { l.logger.Debug(msg, fields...) }
func (l bgLogger) Info(msg string, fields ...interface{})  { l.logger.Info(msg, fields...) }
func (l bgLogger) Warn(msg string, fields ...interface{})  { l.logger.Warn(msg, fields...) }
func (l bgLogger) Error(msg string, fields ...interface{}) { l.logger.Error(msg, fields...) }
func (l bgLogger) Crit(msg string, fields ...interface{})  { l.logger.Crit(msg, fields...) }

type spanLogger struct {
	logger Logger
	span   opentracing.Span
}

func (l spanLogger) logToSpan(level string, msg string, fields ...interface{}) error {
	// Two fields constitute a tlog.Field
	if len(fields)%2 != 0 {
		return errNotEvenFields
	}
	fs := make([]tlog.Field, 0, len(fields)/2+2)
	fs = append(fs, tlog.String("level", level))
	fs = append(fs, tlog.String("message", msg))
	for i := 0; i < len(fields); i += 2 {
		k, ok := fields[i].(string)
		if !ok {
			return errFieldType
		}
		v := fields[i+1]
		switch n := v.(type) {
		case string:
			fs = append(fs, tlog.String(k, n))
		case bool:
			fs = append(fs, tlog.Bool(k, n))
		case int:
			fs = append(fs, tlog.Int(k, n))
		case int32:
			fs = append(fs, tlog.Int32(k, n))
		case uint32:
			fs = append(fs, tlog.Uint32(k, n))
		case int64:
			fs = append(fs, tlog.Int64(k, n))
		case uint64:
			fs = append(fs, tlog.Uint64(k, n))
		case float32:
			fs = append(fs, tlog.Float32(k, n))
		case float64:
			fs = append(fs, tlog.Float64(k, n))
		case error:
			// Assuming you don't pass (*T)(nil).
			if n != nil {
				fs = append(fs, tlog.String(k, n.Error()))
			} else {
				return errFieldType
			}
		case time.Time:
			fs = append(fs, tlog.String(k, n.String()))
		case time.Duration:
			fs = append(fs, tlog.String(k, n.String()))
		default:
			return errFieldType
		}
	}
	l.span.LogFields(fs...)
	return nil
}

func (l spanLogger) Debug(msg string, fields ...interface{}) {
	// Leave "traced" for distinguishing spanLogger and bgLogger. Also any
	// error while logging to span can be recorded locally.
	if err := l.logToSpan("debug", msg, fields...); err != nil {
		l.logger.Debug(msg, append([]interface{}{"traced", err}, fields)...)
	} else {
		l.logger.Debug(msg, append([]interface{}{"traced", "ok"}, fields)...)
	}
}

func (l spanLogger) Info(msg string, fields ...interface{}) {
	if err := l.logToSpan("info", msg, fields...); err != nil {
		l.logger.Info(msg, append([]interface{}{"traced", err}, fields)...)
	} else {
		l.logger.Info(msg, append([]interface{}{"traced", "ok"}, fields)...)
	}
}

func (l spanLogger) Warn(msg string, fields ...interface{}) {
	if err := l.logToSpan("warn", msg, fields...); err != nil {
		l.logger.Warn(msg, append([]interface{}{"traced", err}, fields)...)
	} else {
		l.logger.Warn(msg, append([]interface{}{"traced", "ok"}, fields)...)
	}
}

func (l spanLogger) Error(msg string, fields ...interface{}) {
	if err := l.logToSpan("error", msg, fields...); err != nil {
		l.logger.Error(msg, append([]interface{}{"traced", err}, fields)...)
	} else {
		l.logger.Error(msg, append([]interface{}{"traced", "ok"}, fields)...)
	}
}

func (l spanLogger) Crit(msg string, fields ...interface{}) {
	if err := l.logToSpan("crit", msg, fields...); err != nil {
		l.logger.Crit(msg, append([]interface{}{"traced", err}, fields)...)
	} else {
		l.logger.Crit(msg, append([]interface{}{"traced", "ok"}, fields)...)
	}
}

func contextWithNewSpan(ctx context.Context, tracer opentracing.Tracer,
	ref TracingRef) (context.Context, error) {

	prevSpan := opentracing.SpanFromContext(ctx)
	if prevSpan == nil {
		return ctx, errNoSpan
	}
	var span opentracing.Span
	if ref == TracingChildOf {
		span = tracer.StartSpan(StreamRateLimited,
			opentracing.ChildOf(prevSpan.Context()))
		ctx = opentracing.ContextWithSpan(ctx, span)
	} else if ref == TracingFlowFrom {
		span = tracer.StartSpan(StreamRateLimited,
			opentracing.FollowsFrom(prevSpan.Context()))
		ctx = opentracing.ContextWithSpan(ctx, span)
	} else {
		return ctx, errTracingReference
	}
	return ctx, nil
}
