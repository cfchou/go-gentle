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
// namespace.sRate.name.get_count.result_ok
// namespace.sRate.name.get_count.result_err
// Timing:
// namespace.sRate.name.get_duration.result_ok
// namespace.sRate.name.get_duration.result_err
func RegisterRateLimitedStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_RATELIMITED,
		name, gentle.MX_STREAM_GET}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_RATELIMITED, name)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + "get_count"),
		timing: statter.NewSubStatter(prefix + "get_duration"),
	})
}

// Counter:
// namespace.sRetry.name.get_count.result_ok
// namespace.sRetry.name.get_count.result_err
// namespace.sRetry.name.try_count.result_ok
// namespace.sRetry.name.try_count.result_err
// Timing:
// namespace.sRetry.name.get_duration.result_ok
// namespace.sRetry.name.get_duration.result_err
func RegisterRetryStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_RETRY,
		name, gentle.MX_STREAM_GET}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_RETRY, name)
	if gentle.GetObservation(key) == nil {
		gentle.RegisterObservation(key, &timingObservationImpl{
			count:  statter.NewSubStatter(prefix + "get_count"),
			timing: statter.NewSubStatter(prefix + "get_duration"),
		})
	}
	key = &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_RETRY,
		name, gentle.MX_STREAM_RETRY_TRY}

	if gentle.GetObservation(key) == nil {
		gentle.RegisterObservation(key, &counterImpl{
			count: statter.NewSubStatter(prefix + ".try_count"),
		})
	}
}

// Counter:
// namespace.sBulk.name.get_count.result_ok
// namespace.sBulk.name.get_count.result_err
// Timing:
// namespace.sBulk.name.get_duration.result_ok
// namespace.sBulk.name.get_duration.result_err
func RegisterBulkheadStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_BULKHEAD,
		name, gentle.MX_STREAM_GET}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_BULKHEAD, name)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + "get_count"),
		timing: statter.NewSubStatter(prefix + "get_duration"),
	})
}

// Counter:
// namespace.sCircuit.name.get_count.result_ok
// namespace.sCircuit.name.get_count.result_err
// namespace.sCircuit.name.err_count.err_ErrCircuitOpen
// namespace.sCircuit.name.err_count.err_ErrMaxConcurrency
// namespace.sCircuit.name.err_count.err_ErrTimeout
// namespace.sCircuit.name.err_count.err_NonHystrixErr
// Timing:
// namespace.sCircuit.name.get_duration.result_ok
// namespace.sCircuit.name.get_duration.result_err
func RegisterCircuitBreakerStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_CIRCUITBREAKER,
		name, gentle.MX_STREAM_GET}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_CIRCUITBREAKER, name)
	if gentle.GetObservation(key) == nil {
		gentle.RegisterObservation(key, &timingObservationImpl{
			count:  statter.NewSubStatter(prefix + "get_count"),
			timing: statter.NewSubStatter(prefix + "get_duration"),
		})
	}
	key = &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_CIRCUITBREAKER,
		name,
		gentle.MX_STREAM_CIRCUITBREAKER_HXERR}
	if gentle.GetObservation(key) == nil {
		gentle.RegisterObservation(key, &counterImpl{
			count: statter.NewSubStatter(prefix + ".err_count"),
		})
	}
}

// Counter:
// namespace.sChan.name.get_count.result_ok
// namespace.sChan.name.get_count.result_err
// Timing:
// namespace.sChan.name.get_duration.result_ok
// namespace.sChan.name.get_duration.result_err
func RegisterChannelStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_CHANNEL,
		name, gentle.MX_STREAM_GET}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_CHANNEL, name)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + "get_count"),
		timing: statter.NewSubStatter(prefix + "get_duration"),
	})
}

// Counter:
// namespace.sCon.name.get_count.result_ok
// namespace.sCon.name.get_count.result_err
// Timing:
// namespace.sCon.name.get_duration.result_ok
// namespace.sCon.name.get_duration.result_err
func RegisterConcurrentFetchStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_CONCURRENTFETCH,
		name, gentle.MX_STREAM_GET}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_CONCURRENTFETCH, name)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + "get_count"),
		timing: statter.NewSubStatter(prefix + "get_duration"),
	})
}

// Counter:
// namespace.sMap.name.get_count.result_ok
// namespace.sMap.name.get_count.result_err
// Timing:
// namespace.sMap.name.get_duration.result_ok
// namespace.sMap.name.get_duration.result_err
func RegisterMappedStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_MAPPED,
		name, gentle.MX_STREAM_GET}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_MAPPED, name)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + "get_count"),
		timing: statter.NewSubStatter(prefix + "get_duration"),
	})
}
