package metrics_prometheus

import (
	"github.com/cfchou/go-gentle/gentle"
	prom "github.com/prometheus/client_golang/prometheus"
)

// Histogram:
// namespace_sRate_get_seconds{name, result}
func NewRateLimitedStreamOpts(namespace, name string, limiter gentle.RateLimit) *gentle.RateLimitedStreamOpts {

	opts := gentle.NewRateLimitedStreamOpts(namespace, name, limiter)
	histVec := prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: namespace,
			Subsystem: gentle.MIXIN_STREAM_RATELIMITED,
			Name:      "get_seconds",
			Help:      "Duration of RateLimitedStream.Get() in seconds",
			Buckets:   prom.DefBuckets,
		},
		[]string{"name", "result"})
	prom.MustRegister(histVec)
	opts.MetricGet = &promHist{
		name:    name,
		histVec: histVec,
	}
	return opts
}

// Histogram:
// namespace_sRetry_get_seconds{name, result}
// namespace_sRetry_try_total{name, result}
func NewRetryStreamOpts(namespace, name string, backoff gentle.BackOff,
	tryBuckets []float64) *gentle.RetryStreamOpts {

	opts := gentle.NewRetryStreamOpts(namespace, name, backoff)
	histVec := prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: namespace,
			Subsystem: gentle.MIXIN_STREAM_RETRY,
			Name:      "get_seconds",
			Help:      "Duration of RetryStream.Get() in seconds",
			Buckets:   prom.DefBuckets,
		},
		[]string{"name", "result"})
	prom.MustRegister(histVec)
	opts.MetricGet = &promHist{ name:    name,
		histVec: histVec,
	}

	histVec = prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: namespace,
			Subsystem: gentle.MIXIN_STREAM_RETRY,
			Name:      "try_total",
			Help:      "Number of tries of RetryStream.Get()",
			Buckets:   tryBuckets,
		},
		[]string{"name", "result"})
	prom.MustRegister(histVec)
	opts.MetricTryNum = &promHist{
		name:    name,
		histVec: histVec,
	}
	return opts
}

// Histogram:
// namespace_sBulk_get_seconds{name, result}
func NewBulkStreamOpts(namespace, name string, max_concurrency int) *gentle.BulkheadStreamOpts {

	opts := gentle.NewBulkheadStreamOpts(namespace, name, max_concurrency)
	histVec := prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: namespace,
			Subsystem: gentle.MIXIN_STREAM_BULKHEAD,
			Name:      "get_seconds",
			Help:      "Duration of BulkheadStream.Get() in seconds",
			Buckets:   prom.DefBuckets,
		},
		[]string{"name", "result"})
	prom.MustRegister(histVec)
	opts.MetricGet = &promHist{
		name:    name,
		histVec: histVec,
	}
	return opts
}

// Histogram:
// namespace_sCircuit_get_seconds{name, result}
// Counter:
// namespace_sCircuit_errors_total{name, err}
func NewCircuitBreakerStreamOpts(namespace, name string, circuit string) *gentle.CircuitBreakerStreamOpts {

	opts := gentle.NewCircuitBreakerStreamOpts(namespace, name, circuit)
	histVec := prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: namespace,
			Subsystem: gentle.MIXIN_STREAM_CIRCUITBREAKER,
			Name:      "get_seconds",
			Help:      "Duration of CircuitBreakerStream.Get() in seconds",
			Buckets:   prom.DefBuckets,
		},
		[]string{"name", "result"})
	prom.MustRegister(histVec)
	opts.MetricGet = &promHist{
		name:    name,
		histVec: histVec,
	}

	counterVec := prom.NewCounterVec(
		prom.CounterOpts{
			Namespace: namespace,
			Subsystem: gentle.MIXIN_STREAM_CIRCUITBREAKER,
			Name:      "errors_total",
			Help:      "Number of errors from hystrix.Do() in CircuitBreakerStream.Get()",
		},
		[]string{"name", "err"})
	prom.MustRegister(counterVec)
	opts.MetricCbErr = &promCounter{
			name:       name,
			counterVec: counterVec,
	}
	return opts
}

// Histogram:
// namespace_sChan_get_seconds{name, result}
func NewChannelStreamOpts(namespace, name string, channel <-chan interface{}) *gentle.ChannelStreamOpts {

	opts := gentle.NewChannelStreamOpts(namespace, name, channel)
	histVec := prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: namespace,
			Subsystem: gentle.MIXIN_STREAM_CHANNEL,
			Name:      "get_seconds",
			Help:      "Duration of ChannelStream.Get() in seconds",
			Buckets:   prom.DefBuckets,
		},
		[]string{"name", "result"})
	prom.MustRegister(histVec)
	opts.MetricGet = &promHist{
		name:    name,
		histVec: histVec,
	}
	return opts
}

// Histogram:
// namespace_sHan_get_seconds{name, result}
func NewHandlerStreamOpts(namespace, name string) *gentle.HandlerStreamOpts {

	opts := gentle.NewHandlerStreamOpts(namespace, name)
	histVec := prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: namespace,
			Subsystem: gentle.MIXIN_STREAM_HANDLED,
			Name:      "get_seconds",
			Help:      "Duration of HandlerStream.Get() in seconds",
			Buckets:   prom.DefBuckets,
		},
		[]string{"name", "result"})
	prom.MustRegister(histVec)
	opts.MetricGet = &promHist{
		name:    name,
		histVec: histVec,
	}
	return opts
}

// Histogram:
// namespace_sTrans_get_seconds{name, result}
func NewTransformStreamOpts(namespace, name string,
	transFunc func(gentle.Message, error) (gentle.Message, error)) *gentle.TransformStreamOpts {

	opts := gentle.NewTransformStreamOpts(namespace, name, transFunc)
	histVec := prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: namespace,
			Subsystem: gentle.MIXIN_STREAM_TRANS,
			Name:      "get_seconds",
			Help:      "Duration of TransformStream.Get() in seconds",
			Buckets:   prom.DefBuckets,
		},
		[]string{"name", "result"})
	prom.MustRegister(histVec)
	opts.MetricGet = &promHist{
		name:    name,
		histVec: histVec,
	}
	return opts
}

