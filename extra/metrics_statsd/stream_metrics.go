package metrics_statsd

import (
	"github.com/cactus/go-statsd-client/statsd"
	"fmt"
	"github.com/cfchou/go-gentle/gentle"
)

// Timing maintains a counter too but it doesn't flush every interval. More
// info:
// https://github.com/etsy/statsd/issues/22
type timingObservationImpl struct {
	count statsd.SubStatter
	timing statsd.SubStatter
}

func (p *timingObservationImpl) Observe(value float64, labels map[string]string) {
	for k, v := range labels {
		suffix := k + "_" + v
		p.count.Inc(suffix, 1, 1.0)
		p.timing.Timing(suffix, int64(value * 1000), 1.0)
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
// namespace.s_rate.name.get_count.result_ok
// namespace.s_rate.name.get_count.result_err
// Timing:
// namespace.s_rate.name.get_duration.result_ok
// namespace.s_rate.name.get_duration.result_err
func RegisterRateLimitedStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_RATELIMITED,
		name, "get"}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_RATELIMITED, name)
	ob := &timingObservationImpl{
		count: statter.NewSubStatter(prefix + "get_count"),
		timing: statter.NewSubStatter(prefix + "get_duration"),
	}
	gentle.RegisterObservation(key, ob)
}


// Counter:
// namespace.s_retry.name.get_count.result_ok
// namespace.s_retry.name.get_count.result_err
// namespace.s_retry.name.try_count.result_ok
// namespace.s_retry.name.try_count.result_err
// Timing:
// namespace.s_retry.name.get_duration.result_ok
// namespace.s_retry.name.get_duration.result_err
func RegisterRetryStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_RETRY,
		name, "get"}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_RETRY, name)
	if gentle.GetObservation(key) == nil {
		ob := &timingObservationImpl{
			count: statter.NewSubStatter(prefix + ".get_count"),
			timing: statter.NewSubStatter(prefix + ".get_duration"),
		}
		gentle.RegisterObservation(key, ob)
	}
	key = &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_RETRY,
		name, "try"}

	if gentle.GetCounter(key) == nil {
		counter := &counterImpl{
			count: statter.NewSubStatter(prefix + ".try_count"),
		}
		gentle.RegisterCounter(key, counter)
	}
}

// Counter:
// namespace.s_bulk.name.get_count.result_ok
// namespace.s_bulk.name.get_count.result_err
// Timing:
// namespace.s_bulk.name.get_duration.result_ok
// namespace.s_bulk.name.get_duration.result_err
func RegisterBulkStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_BULKHEAD,
		name, "get"}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_BULKHEAD, name)
	ob := &timingObservationImpl{
		count: statter.NewSubStatter(prefix + ".get_count"),
		timing: statter.NewSubStatter(prefix + ".get_duration"),
	}
	gentle.RegisterObservation(key, ob)
}

// Counter:
// namespace.s_circuit.name.get_count.result_ok
// namespace.s_circuit.name.get_count.result_err
// namespace.s_circuit.name.err_count.err_ErrCircuitOpen
// namespace.s_circuit.name.err_count.err_ErrMaxConcurrency
// namespace.s_circuit.name.err_count.err_ErrTimeout
// namespace.s_circuit.name.err_count.err_NonHystrixErr
// Timing:
// namespace.s_circuit.name.get_duration.result_ok
// namespace.s_circuit.name.get_duration.result_err
func RegisterCircuitBreakerStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_CIRCUITBREAKER,
		name, "get"}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_CIRCUITBREAKER, name)
	if gentle.GetObservation(key) == nil {
		ob := &timingObservationImpl{
			count: statter.NewSubStatter(prefix + ".get_count"),
			timing: statter.NewSubStatter(prefix + ".get_duration"),
		}
		gentle.RegisterObservation(key, ob)
	}
	key = &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_CIRCUITBREAKER,
		name, "hystrix_err"}
	if gentle.GetCounter(key) == nil {
		counter := &counterImpl{
			count: statter.NewSubStatter(prefix + ".err_count"),
		}
		gentle.RegisterCounter(key, counter)
	}
}

// Counter:
// namespace.s_chan.name.get_count.result_ok
// namespace.s_chan.name.get_count.result_err
// Timing:
// namespace.s_chan.name.get_duration.result_ok
// namespace.s_chan.name.get_duration.result_err
func RegisterChannelStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_CHANNEL,
		name, "get"}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_CHANNEL, name)
	ob := &timingObservationImpl{
		count: statter.NewSubStatter(prefix + ".get_count"),
		timing: statter.NewSubStatter(prefix + ".get_duration"),
	}
	gentle.RegisterObservation(key, ob)
}

// Counter:
// namespace.s_con.name.get_count.result_ok
// namespace.s_con.name.get_count.result_err
// Timing:
// namespace.s_con.name.get_duration.result_ok
// namespace.s_con.name.get_duration.result_err
func RegisterConcurrentFetchStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_CONCURRENTFETCH,
		name, "get"}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_CONCURRENTFETCH, name)
	ob := &timingObservationImpl{
		count: statter.NewSubStatter(prefix + ".get_count"),
		timing: statter.NewSubStatter(prefix + ".get_duration"),
	}
	gentle.RegisterObservation(key, ob)
}

// Counter:
// namespace.s_map.name.get_count.result_ok
// namespace.s_map.name.get_count.result_err
// Timing:
// namespace.s_map.name.get_duration.result_ok
// namespace.s_map.name.get_duration.result_err
func RegisterMappedStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_MAPPED,
		name, "get"}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_MAPPED, name)
	ob := &timingObservationImpl{
		count: statter.NewSubStatter(prefix + ".get_count"),
		timing: statter.NewSubStatter(prefix + ".get_duration"),
	}
	gentle.RegisterObservation(key, ob)
}

