package metrics_prometheus

import (
	"github.com/cfchou/go-gentle/gentle"
	prom "github.com/prometheus/client_golang/prometheus"
)

// Histogram:
// namespace_hRate_handle_seconds{name, result}
func RegisterRateLimitedHandlerMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_RATELIMITED,
		name, gentle.MX_HANDLER_HANDLE}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
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
	gentle.RegisterObservation(key, &promHist{
		name:    name,
		histVec: histVec,
	})
}

// Histogram:
// namespace_hRetry_handle_seconds{name, result}
// namespace_hRetry_try_total{name, result}
func RegisterRetryHandlerMetrics(namespace, name string, tryBuckets []float64) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_RETRY,
		name, gentle.MX_HANDLER_HANDLE}
	if _, err := gentle.GetObservation(key); err != nil {
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
		gentle.RegisterObservation(key, &promHist{
			name:    name,
			histVec: histVec,
		})
	}
	key = &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_RETRY,
		name, gentle.MX_HANDLER_RETRY_TRY}
	if _, err := gentle.GetObservation(key); err != nil {
		histVec := prom.NewHistogramVec(
			prom.HistogramOpts{
				Namespace: namespace,
				Subsystem: gentle.MIXIN_HANDLER_RETRY,
				Name:      "try_total",
				Help:      "Number of tries of RetryHandler.Handle()",
				Buckets:   tryBuckets,
			},
			[]string{"name", "result"})
		prom.MustRegister(histVec)
		gentle.RegisterObservation(key, &promHist{
			name:    name,
			histVec: histVec,
		})
	}
}

// Histogram:
// namespace_hBulk_handle_seconds{name, result}
func RegisterBulkheadHandlerMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_BULKHEAD,
		name, gentle.MX_HANDLER_HANDLE}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
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
	gentle.RegisterObservation(key, &promHist{
		name:    name,
		histVec: histVec,
	})
}

// Histogram:
// namespace_hCircuit_handle_seconds{name, result}
// Counter:
// namespace_hCircuit_errors_total{name, err}
func RegisterCircuitBreakerHandlerMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_CIRCUITBREAKER,
		name, gentle.MX_HANDLER_HANDLE}
	if _, err := gentle.GetObservation(key); err != nil {
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
		gentle.RegisterObservation(key, &promHist{
			name:    name,
			histVec: histVec,
		})
	}
	key = &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_CIRCUITBREAKER,
		name,
		gentle.MX_HANDLER_CIRCUITBREAKER_HXERR}
	if _, err := gentle.GetObservation(key); err != nil {
		counterVec := prom.NewCounterVec(
			prom.CounterOpts{
				Namespace: namespace,
				Subsystem: gentle.MIXIN_HANDLER_CIRCUITBREAKER,
				Name:      "errors_total",
				Help:      "Number of errors from hystrix.Do() in CircuitBreakerHandler.Handle()",
			},
			[]string{"name", "err"})
		prom.MustRegister(counterVec)
		counter := &promCounter{
			name:       name,
			counterVec: counterVec,
		}
		gentle.RegisterObservation(key, counter)
	}
}

// Histogram:
// namespace_hTrans_handle_seconds{name, result}
func RegisterTransformHandlerMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_TRANS,
		name, gentle.MX_HANDLER_HANDLE}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
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
	gentle.RegisterObservation(key, &promHist{
		name:    name,
		histVec: histVec,
	})
}
