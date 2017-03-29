package metrics_statsd

import (
	"github.com/cactus/go-statsd-client/statsd"
	"../../gentle"
	"fmt"
)

type timingObservation struct {
	dura statsd.SubStatter
	count statsd.SubStatter
}

func (p *timingObservation) Observe(value float64, labels map[string]string) {
	for k, v := range labels {
		suffix := k + "." + v
		p.dura.Timing(suffix, int64(value * 1000), 1.0)
		p.count.Inc(suffix, 1, 1.0)
	}
}

type gaugeObservation struct {
	gauge statsd.SubStatter
}

func (p *gaugeObservation) Observe(value float64, labels map[string]string) {
	for k, v := range labels {
		suffix := k + "." + v
		p.gauge.Gauge(suffix, int64(value), 1.0)
	}
}

type statsdCounter struct {
	count statsd.SubStatter
}

func (p *statsdCounter) Add(value float64, labels map[string]string) {
	for k, v := range labels {
		suffix := k + "." + v
		p.count.Inc(suffix, 1, 1.0)
	}
}

// namespace.s_rate.get_duration.name.result.ok
// namespace.s_rate.get_count.name.result.ok
// namespace.s_rate.get_duration.name.result.err
// namespace.s_rate.get_count.name.result.err
func RegisterRateLimitedStreamMetrics(client statsd.Client, namespace, name string) {
	key := &gentle.RegistryKey{namespace, name,
				   gentle.MIXIN_STREAM_RATELIMITED, "get"}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	ob := &timingObservation{
		dura: client.NewSubStatter(fmt.Sprintf(
			"%s.%s.get_duration.%s", namespace,
			gentle.MIXIN_STREAM_RATELIMITED, name)),
		count: client.NewSubStatter(fmt.Sprintf(
			"%s.%s.get_count.%s", namespace,
			gentle.MIXIN_STREAM_RATELIMITED, name)),
	}
	gentle.RegisterObservation(key, ob)
}


// namespace.s_retry.get_duration.name.result.ok
// namespace.s_retry.get_count.name.result.ok
// namespace.s_retry.get_duration.name.result.err
// namespace.s_retry.get_count.name.result.err
// namespace.s_retry.tries.name.result.ok
// namespace.s_retry.tries.name.result.err
func RegisterRetryStreamMetrics(client statsd.Client, namespace, name string, tryBuckets []float64) {
	key := &gentle.RegistryKey{namespace, name,
				   gentle.MIXIN_STREAM_RETRY, "get"}
	if gentle.GetObservation(key) == nil {
		ob := &timingObservation{
			dura: client.NewSubStatter(fmt.Sprintf(
				"%s.%s.get_duration.%s", namespace,
				gentle.MIXIN_STREAM_RETRY, name)),
			count: client.NewSubStatter(fmt.Sprintf(
				"%s.%s.get_count.%s", namespace,
				gentle.MIXIN_STREAM_RETRY, name)),
		}
		gentle.RegisterObservation(key, ob)
	}
	key = &gentle.RegistryKey{namespace, name,
				  gentle.MIXIN_STREAM_RETRY, "try"}

	if gentle.GetObservation(key) == nil {
		ob := &gaugeObservation{
			gauge: client.NewSubStatter(fmt.Sprintf(
				"%s.%s.tries.%s", namespace,
				gentle.MIXIN_STREAM_RETRY, name)),
		}
		gentle.RegisterObservation(key, ob)
	}
}

// namespace.s_bulk.get_duration.name.result.ok
// namespace.s_bulk.get_count.name.result.ok
// namespace.s_bulk.get_duration.name.result.err
// namespace.s_bulk.get_count.name.result.err
func RegisterBulkStreamMetrics(client statsd.Client, namespace, name string) {
	key := &gentle.RegistryKey{namespace, name,
				   gentle.MIXIN_STREAM_BULKHEAD, "get"}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	ob := &timingObservation{
		dura: client.NewSubStatter(fmt.Sprintf(
			"%s.%s.get_duration.%s", namespace,
			gentle.MIXIN_STREAM_BULKHEAD, name)),
		count: client.NewSubStatter(fmt.Sprintf(
			"%s.%s.get_count.%s", namespace,
			gentle.MIXIN_STREAM_BULKHEAD, name)),
	}
	gentle.RegisterObservation(key, ob)
}

// namespace_s_circuit_errors_total{name, err}

// namespace.s_circuit.get_duration.name.result.ok
// namespace.s_circuit.get_count.name.result.ok
// namespace.s_circuit.get_duration.name.result.err
// namespace.s_circuit.get_count.name.result.err

// namespace.s_circuit.error_count.name.err.ErrCircuitOpen
// namespace.s_circuit.error_count.name.err.ErrMaxConcurrency
// namespace.s_circuit.error_count.name.err.ErrTimeout
// namespace.s_circuit.error_count.name.err.NonHystrixErr
func RegisterCircuitBreakerStreamMetrics(client statsd.Client, namespace, name string) {
	key := &gentle.RegistryKey{namespace, name,
				   gentle.MIXIN_STREAM_CIRCUITBREAKER, "get"}
	if gentle.GetObservation(key) == nil {
		ob := &timingObservation{
			dura: client.NewSubStatter(fmt.Sprintf(
				"%s.%s.get_duration.%s", namespace,
				gentle.MIXIN_STREAM_CIRCUITBREAKER, name)),
			count: client.NewSubStatter(fmt.Sprintf(
				"%s.%s.get_count.%s", namespace,
				gentle.MIXIN_STREAM_CIRCUITBREAKER, name)),
		}
		gentle.RegisterObservation(key, ob)
	}
	key = &gentle.RegistryKey{namespace, name,
				  gentle.MIXIN_STREAM_CIRCUITBREAKER, "hystrix_err"}
	if gentle.GetCounter(key) == nil {
		counter := &statsdCounter{
			count: client.NewSubStatter(fmt.Sprintf(
				"%s.%s.error_count.%s", namespace,
				gentle.MIXIN_STREAM_CIRCUITBREAKER, name)),
		}
		gentle.RegisterCounter(key, counter)
	}
}

// namespace.s_chan.get_duration.name.result.ok
// namespace.s_chan.get_count.name.result.ok
// namespace.s_chan.get_duration.name.result.err
// namespace.s_chan.get_count.name.result.err
func RegisterChannelStreamMetrics(client statsd.Client, namespace, name string) {
	key := &gentle.RegistryKey{namespace, name,
				   gentle.MIXIN_STREAM_CHANNEL, "get"}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	ob := &timingObservation{
		dura: client.NewSubStatter(fmt.Sprintf(
			"%s.%s.get_duration.%s", namespace,
			gentle.MIXIN_STREAM_CHANNEL, name)),
		count: client.NewSubStatter(fmt.Sprintf(
			"%s.%s.get_count.%s", namespace,
			gentle.MIXIN_STREAM_CHANNEL, name)),
	}
	gentle.RegisterObservation(key, ob)
}

// namespace.s_con.get_duration.name.result.ok
// namespace.s_con.get_count.name.result.ok
// namespace.s_con.get_duration.name.result.err
// namespace.s_con.get_count.name.result.err
func RegisterConcurrentFetchStreamMetrics(client statsd.Client, namespace, name string) {
	key := &gentle.RegistryKey{namespace, name,
				   gentle.MIXIN_STREAM_CONCURRENTFETCH, "get"}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	ob := &timingObservation{
		dura: client.NewSubStatter(fmt.Sprintf(
			"%s.%s.get_duration.%s", namespace,
			gentle.MIXIN_STREAM_CONCURRENTFETCH, name)),
		count: client.NewSubStatter(fmt.Sprintf(
			"%s.%s.get_count.%s", namespace,
			gentle.MIXIN_STREAM_CONCURRENTFETCH, name)),
	}
	gentle.RegisterObservation(key, ob)
}

// namespace.s_map.get_duration.name.result.ok
// namespace.s_map.get_count.name.result.ok
// namespace.s_map.get_duration.name.result.err
// namespace.s_map.get_count.name.result.err
func RegisterMappedStreamMetrics(client statsd.Client, namespace, name string) {
	key := &gentle.RegistryKey{namespace, name,
				   gentle.MIXIN_STREAM_MAPPED, "get"}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	ob := &timingObservation{
		dura: client.NewSubStatter(fmt.Sprintf(
			"%s.%s.get_duration.%s", namespace,
			gentle.MIXIN_STREAM_MAPPED, name)),
		count: client.NewSubStatter(fmt.Sprintf(
			"%s.%s.get_count.%s", namespace,
			gentle.MIXIN_STREAM_MAPPED, name)),
	}
	gentle.RegisterObservation(key, ob)
}

