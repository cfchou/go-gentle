package main

import (
	"github.com/uber/jaeger-lib/metrics"
	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	jaegerlog "github.com/uber/jaeger-client-go/log"
	"log"
	"github.com/opentracing/opentracing-go"
	"gopkg.in/cfchou/go-gentle.v3/gentle"
	"context"
	"time"
)

func main() {
	jLogger := jaegerlog.StdLogger
	jMetricsFactory := metrics.NullFactory
	// Sample configuration for testing. Use constant sampling to sample every trace
	// and enable LogSpan to log every span via configured Logger.
	jConf := jaegercfg.Configuration{
		Sampler: &jaegercfg.SamplerConfig{
			Type: jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &jaegercfg.ReporterConfig{
			LogSpans: true,
		},
	}

	// Initialize tracer with a logger and a metrics factory
	closer, err := jConf.InitGlobalTracer(
		"extra",
		jaegercfg.Logger(jLogger),
		jaegercfg.Metrics(jMetricsFactory),
	)
	if err != nil {
		log.Printf("Could not initialize jaeger tracer: %s", err.Error())
		return
	}
	defer closer.Close()

	var upstream gentle.SimpleStream = func(ctx context.Context) (gentle.Message, error) {
		prevSpan := opentracing.SpanFromContext(ctx)
		if prevSpan == nil {
			log.Println("No span in Context")
			return gentle.SimpleMessage(""), nil
		}
		span, ctx := opentracing.StartSpanFromContext(ctx, "Get")
		defer span.Finish()
		time.Sleep(200*time.Millisecond)
		return gentle.SimpleMessage(""), nil
	}
	opts := gentle.NewRateLimitedStreamOpts("", "test1",
		gentle.NewTokenBucketRateLimit(time.Second, 1))
	stream := gentle.NewRateLimitedStream(opts, upstream)

	log.Println("Start")
	for i := 0; i < 10; i++ {
		ctx := context.Background()
		span, ctxTraced := opentracing.StartSpanFromContext(ctx, "Get")
		stream.Get(ctxTraced)
		span.Finish()
	}
	log.Println("End")
}


