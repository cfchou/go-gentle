package metrics_prometheus

import (
	"github.com/cfchou/go-gentle/gentle"
	prom "github.com/prometheus/client_golang/prometheus"
)

// Histogram:
// namespace_hRate_handle_seconds{name, result}
func NewRateLimitedHandlerOpts(namespace, name string, limiter gentle.RateLimit) *gentle.RateLimitedHandlerOpts {

	opts := gentle.NewRateLimitedHandlerOpts(namespace, name, limiter)
	histVec := prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: namespace,
			Subsystem: gentle.MIXIN_HANDLER_RATELIMITED,
			Name:      "handle_seconds",
			Help:      "Duration of RateLimitedHandler.Handle() in seconds",
			Buckets:   prom.DefBuckets,
		},
		[]string{"name", "result"})
	prom.MustRegister(histVec)
	opts.MetricHandle = &promHist{
		name:    name,
		histVec: histVec,
	}
	return opts
}

// Histogram:
// namespace_hRetry_handle_seconds{name, result}
// namespace_hRetry_try_total{name, result}
func NewRetryHandlerOpts(namespace, name string, backoff gentle.BackOff,
	tryBuckets []float64) *gentle.RetryHandlerOpts {

	opts := gentle.NewRetryHandlerOpts(namespace, name, backoff)

	histVec := prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: namespace,
			Subsystem: gentle.MIXIN_HANDLER_RETRY,
			Name:      "handle_seconds",
			Help:      "Duration of RetryHandler.Handle() in seconds",
			Buckets:   prom.DefBuckets,
		},
		[]string{"name", "result"})
	prom.MustRegister(histVec)
	opts.MetricHandle = &promHist{
		name:    name,
		histVec: histVec,
	}
	histVec = prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: namespace,
			Subsystem: gentle.MIXIN_HANDLER_RETRY,
			Name:      "try_total",
			Help:      "Number of tries of RetryHandler.Handle()",
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
// namespace_hBulk_handle_seconds{name, result}
func NewBulkheadHandlerOpts(namespace, name string, max_concurrency int) *gentle.BulkheadHandlerOpts {

	opts := gentle.NewBulkheadHandlerOpts(namespace, name, max_concurrency)
	histVec := prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: namespace,
			Subsystem: gentle.MIXIN_HANDLER_BULKHEAD,
			Name:      "handle_seconds",
			Help:      "Duration of BulkheadHandler.Handle() in seconds",
			Buckets:   prom.DefBuckets,
		},
		[]string{"name", "result"})
	prom.MustRegister(histVec)
	opts.MetricHandle = &promHist{
		name:    name,
		histVec: histVec,
	}
	return opts
}

// Histogram:
// namespace_hCircuit_handle_seconds{name, result}
// Counter:
// namespace_hCircuit_errors_total{name, err}
func NewCircuitBreakerHandlerOpts(namespace, name, circuit string) *gentle.CircuitBreakerHandlerOpts {

	opts := gentle.NewCircuitBreakerHandlerOpts(namespace, name, circuit)
	histVec := prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: namespace,
			Subsystem: gentle.MIXIN_HANDLER_CIRCUITBREAKER,
			Name:      "handle_seconds",
			Help:      "Duration of CircuitBreakerHandler.Handle() in seconds",
			Buckets:   prom.DefBuckets,
		},
		[]string{"name", "result"})
	prom.MustRegister(histVec)
	opts.MetricHandle = &promHist{
		name:    name,
		histVec: histVec,
	}

	counterVec := prom.NewCounterVec(
		prom.CounterOpts{
			Namespace: namespace,
			Subsystem: gentle.MIXIN_HANDLER_CIRCUITBREAKER,
			Name:      "errors_total",
			Help:      "Number of errors from hystrix.Do() in CircuitBreakerHandler.Handle()",
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
// namespace_hTrans_handle_seconds{name, result}
func NewTransformHandlerOpts(namespace, name string,
	transFunc func(gentle.Message, error) (gentle.Message, error)) *gentle.TransformHandlerOpts {

	opts := gentle.NewTransformHandlerOpts(namespace, name, transFunc)
	histVec := prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: namespace,
			Subsystem: gentle.MIXIN_HANDLER_TRANS,
			Name:      "handle_seconds",
			Help:      "Duration of MappedHandler.Handle() in seconds",
			Buckets:   prom.DefBuckets,
		},
		[]string{"name", "result"})
	prom.MustRegister(histVec)
	opts.MetricHandle = &promHist{
		name:    name,
		histVec: histVec,
	}
	return opts
}

