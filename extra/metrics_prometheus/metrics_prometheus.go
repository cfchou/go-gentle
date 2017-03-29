package metrics_prometheus

import (
	//"gopkg.in/cfchou/go-gentle.v1/gentle"
	"../../gentle"
	prom "github.com/prometheus/client_golang/prometheus"
)

type promObeservation struct {
	name    string
	histVec *prom.HistogramVec
}

func (p *promObeservation) Observe(value float64, labels map[string]string) {
	m := map[string]string{"name": p.name}
	for k, v := range labels {
		m[k] = v
	}
	h := p.histVec.With(m)
	h.Observe(value)
}

type promCounter struct {
	name       string
	counterVec *prom.CounterVec
}

func (p *promCounter) Add(value float64, labels map[string]string) {
	m := map[string]string{"name": p.name}
	for k, v := range labels {
		m[k] = v
	}
	c := p.counterVec.With(m)
	c.Add(value)
}

// namespace_s_rate_get_seconds{name, result}
func RegisterRateLimitedStreamMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace, name,
		gentle.MIXIN_STREAM_RATELIMITED, "get"}
	if gentle.GetObservation(key) != nil {
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
	ob := &promObeservation{
		name:    name,
		histVec: histVec,
	}
	gentle.RegisterObservation(key, ob)
}

// namespace_s_retry_get_seconds{name, result}
// namespace_s_retry_tries_total{name, result}
// Given N = RetryStream's len(backoffs)+1, so that N is the maximum of tries.
// tryBuckets have buckets sensibly grouping the range [1, N]. The total number
// of buckets should not be too large. For example,
// if backoffs = [1, 2, 4, 8, 16], then tryBuckets may be [1, 2, 3, 4, 5, 6]
// which makes one try one bucket.
// If backoffs is a large list of 30 elements, then tryBuckets may be
// [1, 2, 4, 8, 16, 24, 32].
func RegisterRetryStreamMetrics(namespace, name string, tryBuckets []float64) {
	key := &gentle.RegistryKey{namespace, name,
		gentle.MIXIN_STREAM_RETRY, "get"}
	if gentle.GetObservation(key) == nil {
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
		ob := &promObeservation{
			name:    name,
			histVec: histVec,
		}
		gentle.RegisterObservation(key, ob)
	}
	key = &gentle.RegistryKey{namespace, name,
		gentle.MIXIN_STREAM_RETRY, "try"}
	if gentle.GetObservation(key) == nil {
		histVec := prom.NewHistogramVec(
			prom.HistogramOpts{
				Namespace: namespace,
				Subsystem: gentle.MIXIN_STREAM_RETRY,
				Name:      "tries_total",
				Help:      "Number of tries of RetryStream.Get()",
				Buckets:   tryBuckets,
			},
			[]string{"name", "result"})
		prom.MustRegister(histVec)
		ob := &promObeservation{
			name:    name,
			histVec: histVec,
		}
		gentle.RegisterObservation(key, ob)
	}
}

// namespace_s_bulk_get_seconds{name, result}
func RegisterBulkStreamMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace, name,
		gentle.MIXIN_STREAM_BULKHEAD, "get"}
	if gentle.GetObservation(key) != nil {
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
	ob := &promObeservation{
		name:    name,
		histVec: histVec,
	}
	gentle.RegisterObservation(key, ob)
}

// namespace_s_circuit_get_seconds{name, result}
// namespace_s_circuit_errors_total{name, err}
func RegisterCircuitBreakerStreamMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace, name,
		gentle.MIXIN_STREAM_CIRCUITBREAKER, "get"}
	if gentle.GetObservation(key) == nil {
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
		ob := &promObeservation{
			name:    name,
			histVec: histVec,
		}
		gentle.RegisterObservation(key, ob)
	}
	key = &gentle.RegistryKey{namespace, name,
		gentle.MIXIN_STREAM_CIRCUITBREAKER, "hystrix_err"}
	if gentle.GetCounter(key) == nil {
		counterVec := prom.NewCounterVec(
			prom.CounterOpts{
				Namespace: namespace,
				Subsystem: gentle.MIXIN_STREAM_CIRCUITBREAKER,
				Name:      "errors_total",
				Help:      "Number of errors from hystrix.Do() in CircuitBreakerStream",
			},
			[]string{"name", "err"})
		prom.MustRegister(counterVec)
		counter := &promCounter{
			name:       name,
			counterVec: counterVec,
		}
		gentle.RegisterCounter(key, counter)
	}
}

// namespace_s_chan_get_seconds{name, result}
func RegisterChannelStreamMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace, name,
		gentle.MIXIN_STREAM_CHANNEL, "get"}
	if gentle.GetObservation(key) != nil {
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
	ob := &promObeservation{
		name:    name,
		histVec: histVec,
	}
	gentle.RegisterObservation(key, ob)
}

// namespace_s_con_get_seconds{name, result}
func RegisterConcurrentFetchStreamMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace, name,
		gentle.MIXIN_STREAM_CONCURRENTFETCH, "get"}
	if gentle.GetObservation(key) != nil {
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
	ob := &promObeservation{
		name:    name,
		histVec: histVec,
	}
	gentle.RegisterObservation(key, ob)
}

// namespace_s_map_get_seconds{name, result}
func RegisterMappedStreamMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace, name,
		gentle.MIXIN_STREAM_MAPPED, "get"}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	histVec := prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: namespace,
			Subsystem: gentle.MIXIN_STREAM_MAPPED,
			Name:      "get_seconds",
			Help:      "Duration of MappedStream.Get() in seconds",
			Buckets:   prom.DefBuckets,
		},
		[]string{"name", "result"})
	prom.MustRegister(histVec)
	ob := &promObeservation{
		name:    name,
		histVec: histVec,
	}
	gentle.RegisterObservation(key, ob)
}
