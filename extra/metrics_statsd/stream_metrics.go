package metrics_statsd

import (
	"fmt"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/cfchou/go-gentle/gentle"
)

// Timing maintains a counter too but it doesn't flush every interval. More
// info:
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

func (p *counterImpl) Add(value float64, labels map[string]string) {
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
		name, gentle.MX_STREAM_OB_GET}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_RATELIMITED, name)
	ob := &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + "get_count"),
		timing: statter.NewSubStatter(prefix + "get_duration"),
	}
	gentle.RegisterObservation(key, ob)
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
		name, gentle.MX_STREAM_OB_GET}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_RETRY, name)
	if gentle.GetObservation(key) == nil {
		ob := &timingObservationImpl{
			count:  statter.NewSubStatter(prefix + ".get_count"),
			timing: statter.NewSubStatter(prefix + ".get_duration"),
		}
		gentle.RegisterObservation(key, ob)
	}
	key = &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_RETRY,
		name, gentle.MX_STREAM_RETRY_OB_TRY}

	if gentle.GetCounter(key) == nil {
		counter := &counterImpl{
			count: statter.NewSubStatter(prefix + ".try_count"),
		}
		gentle.RegisterCounter(key, counter)
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
		name, gentle.MX_STREAM_OB_GET}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_BULKHEAD, name)
	ob := &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + ".get_count"),
		timing: statter.NewSubStatter(prefix + ".get_duration"),
	}
	gentle.RegisterObservation(key, ob)
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
		name, gentle.MX_STREAM_OB_GET}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_CIRCUITBREAKER, name)
	if gentle.GetObservation(key) == nil {
		ob := &timingObservationImpl{
			count:  statter.NewSubStatter(prefix + ".get_count"),
			timing: statter.NewSubStatter(prefix + ".get_duration"),
		}
		gentle.RegisterObservation(key, ob)
	}
	key = &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_CIRCUITBREAKER,
		name,
		gentle.MX_STREAM_CIRCUITBREAKER_CNT_HXERR}
	if gentle.GetCounter(key) == nil {
		counter := &counterImpl{
			count: statter.NewSubStatter(prefix + ".err_count"),
		}
		gentle.RegisterCounter(key, counter)
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
		name, gentle.MX_STREAM_OB_GET}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_CHANNEL, name)
	ob := &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + ".get_count"),
		timing: statter.NewSubStatter(prefix + ".get_duration"),
	}
	gentle.RegisterObservation(key, ob)
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
		name, gentle.MX_STREAM_OB_GET}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_CONCURRENTFETCH, name)
	ob := &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + ".get_count"),
		timing: statter.NewSubStatter(prefix + ".get_duration"),
	}
	gentle.RegisterObservation(key, ob)
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
		name, gentle.MX_STREAM_OB_GET}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_MAPPED, name)
	ob := &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + ".get_count"),
		timing: statter.NewSubStatter(prefix + ".get_duration"),
	}
	gentle.RegisterObservation(key, ob)
}
