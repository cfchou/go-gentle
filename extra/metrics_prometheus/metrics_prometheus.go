package metrics_prometheus

import (
	//"gopkg.in/cfchou/go-gentle.v1/gentle"
	prom "github.com/prometheus/client_golang/prometheus"
	"../../gentle"
	"errors"
)

type PrometheusMetrics struct {

}

func (m *PrometheusMetrics) Register(key gentle.RegistryKey) error {
	switch key.Mixin {
	case gentle.MIXIN_STREAM_RATELIMITED:
	case gentle.MIXIN_STREAM_RETRY:
	case gentle.MIXIN_STREAM_BULKHEAD:
	case gentle.MIXIN_STREAM_CIRCUITBREAKER:
	case gentle.MIXIN_STREAM_CHANNEL:
	case gentle.MIXIN_STREAM_CONCURRENTFETCH:
	case gentle.MIXIN_STREAM_MAPPED:
	default:
		gentle.Log.Error("[Metrics] Unsupported mixin", "mixin", key.Mixin)
		return errors.New("Unsupported mixin")
	}

}


func (m *PrometheusMetrics) registerRateLimitedStreamMetrics(namespace, name string) error {
	HistVec := prom.NewHistogramVec(
		prom.HistogramOpts {
			Namespace: namespace,
			Subsystem: gentle.MIXIN_STREAM_RATELIMITED + "_" + name,
			Name: "get_seconds",
			Help:"Duration of Stream.Get() in seconds",
			Buckets: prom.DefBuckets,
		},
		[]string{"mixin", "name", "result"})

}
