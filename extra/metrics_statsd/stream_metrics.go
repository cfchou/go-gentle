package metrics_statsd

import (
	"fmt"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/cfchou/go-gentle/gentle"
)

// A statsd timing maintains a counter too but it doesn't get flushed every
// interval. Therefore we explicitly set up a statsd counter.
// More info:
// https://github.com/etsy/statsd/issues/22
type timingObservationImpl struct {
	count  statsd.SubStatter
	timing statsd.SubStatter
}

func (p *timingObservationImpl) Observe(value float64, labels map[string]string) {
	for k, v := range labels {
		suffix := k + "_" + v
		p.count.Inc(suffix, 1, 1.0)
		p.timing.Timing(suffix, int64(value*1000), 1.0)
	}
}

type counterImpl struct {
	count statsd.SubStatter
}

func (p *counterImpl) Observe(value float64, labels map[string]string) {
	for k, v := range labels {
		suffix := k + "_" + v
		p.count.Inc(suffix, int64(value), 1.0)
	}
}

// Counter:
// namespace.sRate.name.get.result_ok
// namespace.sRate.name.get.result_err
// Timing:
// namespace.sRate.name.get.result_ok
// namespace.sRate.name.get.result_err
func RegisterRateLimitedStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_RATELIMITED,
		name, gentle.MX_STREAM_GET}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_RATELIMITED, name)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + "get"),
		timing: statter.NewSubStatter(prefix + "get"),
	})
}

// Counter:
// namespace.sRetry.name.get.result_ok
// namespace.sRetry.name.get.result_err
// namespace.sRetry.name.try.result_ok
// namespace.sRetry.name.try.result_err
// Timing:
// namespace.sRetry.name.get.result_ok
// namespace.sRetry.name.get.result_err
func RegisterRetryStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_RETRY,
		name, gentle.MX_STREAM_GET}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_RETRY, name)
	if _, err := gentle.GetObservation(key); err == nil {
		gentle.RegisterObservation(key, &timingObservationImpl{
			count:  statter.NewSubStatter(prefix + "get"),
			timing: statter.NewSubStatter(prefix + "get"),
		})
	}
	key = &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_RETRY,
		name, gentle.MX_STREAM_RETRY_TRY}

	if _, err := gentle.GetObservation(key); err == nil {
		gentle.RegisterObservation(key, &counterImpl{
			count: statter.NewSubStatter(prefix + ".try"),
		})
	}
}

// Counter:
// namespace.sBulk.name.get.result_ok
// namespace.sBulk.name.get.result_err
// Timing:
// namespace.sBulk.name.get.result_ok
// namespace.sBulk.name.get.result_err
func RegisterBulkheadStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_BULKHEAD,
		name, gentle.MX_STREAM_GET}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_BULKHEAD, name)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + "get"),
		timing: statter.NewSubStatter(prefix + "get"),
	})
}

// Counter:
// namespace.sCircuit.name.get.result_ok
// namespace.sCircuit.name.get.result_err
// namespace.sCircuit.name.err.err_ErrCircuitOpen
// namespace.sCircuit.name.err.err_ErrMaxConcurrency
// namespace.sCircuit.name.err.err_ErrTimeout
// namespace.sCircuit.name.err.err_NonHystrixErr
// Timing:
// namespace.sCircuit.name.get.result_ok
// namespace.sCircuit.name.get.result_err
func RegisterCircuitBreakerStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_CIRCUITBREAKER,
		name, gentle.MX_STREAM_GET}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_CIRCUITBREAKER, name)
	if _, err := gentle.GetObservation(key); err == nil {
		gentle.RegisterObservation(key, &timingObservationImpl{
			count:  statter.NewSubStatter(prefix + "get"),
			timing: statter.NewSubStatter(prefix + "get"),
		})
	}
	key = &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_CIRCUITBREAKER,
		name,
		gentle.MX_STREAM_CIRCUITBREAKER_HXERR}
	if _, err := gentle.GetObservation(key); err == nil {
		gentle.RegisterObservation(key, &counterImpl{
			count: statter.NewSubStatter(prefix + ".err"),
		})
	}
}

// Counter:
// namespace.sChan.name.get.result_ok
// namespace.sChan.name.get.result_err
// Timing:
// namespace.sChan.name.get.result_ok
// namespace.sChan.name.get.result_err
func RegisterChannelStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_CHANNEL,
		name, gentle.MX_STREAM_GET}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_CHANNEL, name)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + "get"),
		timing: statter.NewSubStatter(prefix + "get"),
	})
}

// Counter:
// namespace.sCon.name.get.result_ok
// namespace.sCon.name.get.result_err
// Timing:
// namespace.sCon.name.get.result_ok
// namespace.sCon.name.get.result_err
func RegisterConcurrentFetchStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_CONCURRENTFETCH,
		name, gentle.MX_STREAM_GET}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_CONCURRENTFETCH, name)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + "get"),
		timing: statter.NewSubStatter(prefix + "get"),
	})
}

// Counter:
// namespace.sMap.name.get.result_ok
// namespace.sMap.name.get.result_err
// Timing:
// namespace.sMap.name.get.result_ok
// namespace.sMap.name.get.result_err
func RegisterMappedStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_MAPPED,
		name, gentle.MX_STREAM_GET}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_MAPPED, name)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + "get"),
		timing: statter.NewSubStatter(prefix + "get"),
	})
}
