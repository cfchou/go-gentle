package metrics_prometheus

import (
	"github.com/cfchou/go-gentle/gentle"
	prom "github.com/prometheus/client_golang/prometheus"
)

// Histogram:
// namespace_sRate_get_seconds{name, result}
func RegisterRateLimitedStreamMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_RATELIMITED,
		name, gentle.MX_STREAM_GET}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
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
	gentle.RegisterObservation(key, &promHist{
		name:    name,
		histVec: histVec,
	})
}

// Histogram:
// namespace_sRetry_get_seconds{name, result}
// namespace_sRetry_try_total{name, result}
func RegisterRetryStreamMetrics(namespace, name string, tryBuckets []float64) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_RETRY,
		name, gentle.MX_STREAM_GET}
	if _, err := gentle.GetObservation(key); err != nil {
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
		gentle.RegisterObservation(key, &promHist{
			name:    name,
			histVec: histVec,
		})
	}
	key = &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_RETRY,
		name, gentle.MX_STREAM_RETRY_TRY}
	if _, err := gentle.GetObservation(key); err != nil {
		histVec := prom.NewHistogramVec(
			prom.HistogramOpts{
				Namespace: namespace,
				Subsystem: gentle.MIXIN_STREAM_RETRY,
				Name:      "try_total",
				Help:      "Number of tries of RetryStream.Get()",
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
// namespace_sBulk_get_seconds{name, result}
func RegisterBulkheadStreamMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_BULKHEAD,
		name, gentle.MX_STREAM_GET}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
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
	gentle.RegisterObservation(key, &promHist{
		name:    name,
		histVec: histVec,
	})
}

// Histogram:
// namespace_sCircuit_get_seconds{name, result}
// Counter:
// namespace_sCircuit_errors_total{name, err}
func RegisterCircuitBreakerStreamMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_CIRCUITBREAKER,
		name, gentle.MX_STREAM_GET}
	if _, err := gentle.GetObservation(key); err != nil {
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
		gentle.RegisterObservation(key, &promHist{
			name:    name,
			histVec: histVec,
		})
	}
	key = &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_CIRCUITBREAKER,
		name,
		gentle.MX_STREAM_CIRCUITBREAKER_HXERR}
	if _, err := gentle.GetObservation(key); err != nil {
		counterVec := prom.NewCounterVec(
			prom.CounterOpts{
				Namespace: namespace,
				Subsystem: gentle.MIXIN_STREAM_CIRCUITBREAKER,
				Name:      "errors_total",
				Help:      "Number of errors from hystrix.Do() in CircuitBreakerStream.Get()",
			},
			[]string{"name", "err"})
		prom.MustRegister(counterVec)
		gentle.RegisterObservation(key, &promCounter{
			name:       name,
			counterVec: counterVec,
		})
	}
}

// Histogram:
// namespace_sChan_get_seconds{name, result}
func RegisterChannelStreamMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_CHANNEL,
		name, gentle.MX_STREAM_GET}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
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
	gentle.RegisterObservation(key, &promHist{
		name:    name,
		histVec: histVec,
	})
}

// Histogram:
// namespace_sCon_get_seconds{name, result}
func RegisterConcurrentFetchStreamMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_CONCURRENTFETCH,
		name, gentle.MX_STREAM_GET}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
	histVec := prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: namespace,
			Subsystem: gentle.MIXIN_STREAM_CONCURRENTFETCH,
			Name:      "get_seconds",
			Help:      "Duration of ConcurrentFetchStream.Get() in seconds",
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
// namespace_sHan_get_seconds{name, result}
func RegisterHandlerStreamMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_HANDLED,
		name, gentle.MX_STREAM_GET}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
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
	gentle.RegisterObservation(key, &promHist{
		name:    name,
		histVec: histVec,
	})
}

// Histogram:
// namespace_sTrans_get_seconds{name, result}
func RegisterTransformStreamMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_TRANS,
		name, gentle.MX_STREAM_GET}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
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
	gentle.RegisterObservation(key, &promHist{
		name:    name,
		histVec: histVec,
	})
}
