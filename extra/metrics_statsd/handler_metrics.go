package metrics_statsd

import (
	"fmt"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/cfchou/go-gentle/gentle"
)

// Counter:
// namespace.hRate.name.get_count.result_ok
// namespace.hRate.name.get_count.result_err
// Timing:
// namespace.hRate.name.get_duration.result_ok
// namespace.hRate.name.get_duration.result_err
func RegisterRateLimitedHandlerMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_RATELIMITED,
		name, gentle.MX_HANDLER_OB_HANDLE}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_RATELIMITED, name)
	ob := &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + "get_count"),
		timing: statter.NewSubStatter(prefix + "get_duration"),
	}
	gentle.RegisterObservation(key, ob)
}

// Counter:
// namespace.hRetry.name.get_count.result_ok
// namespace.hRetry.name.get_count.result_err
// namespace.hRetry.name.try_count.result_ok
// namespace.hRetry.name.try_count.result_err
// Timing:
// namespace.hRetry.name.get_duration.result_ok
// namespace.hRetry.name.get_duration.result_err
func RegisterRetryHandlerMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_RETRY,
		name, gentle.MX_HANDLER_OB_HANDLE}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_RETRY, name)
	if gentle.GetObservation(key) == nil {
		ob := &timingObservationImpl{
			count:  statter.NewSubStatter(prefix + ".get_count"),
			timing: statter.NewSubStatter(prefix + ".get_duration"),
		}
		gentle.RegisterObservation(key, ob)
	}
	key = &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_RETRY,
		name, gentle.MX_HANDLER_RETRY_OB_TRY}

	if gentle.GetCounter(key) == nil {
		counter := &counterImpl{
			count: statter.NewSubStatter(prefix + ".try_count"),
		}
		gentle.RegisterCounter(key, counter)
	}
}

// Counter:
// namespace.hBulk.name.get_count.result_ok
// namespace.hBulk.name.get_count.result_err
// Timing:
// namespace.hBulk.name.get_duration.result_ok
// namespace.hBulk.name.get_duration.result_err
func RegisterBulkheadHandlerMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_BULKHEAD,
		name, gentle.MX_HANDLER_OB_HANDLE}
	if gentle.GetObservation(key) != nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_BULKHEAD, name)
	ob := &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + ".get_count"),
		timing: statter.NewSubStatter(prefix + ".get_duration"),
	}
	gentle.RegisterObservation(key, ob)
}

// Counter:
// namespace.hCircuit.name.get_count.result_ok
// namespace.hCircuit.name.get_count.result_err
// namespace.hCircuit.name.err_count.err_ErrCircuitOpen
// namespace.hCircuit.name.err_count.err_ErrMaxConcurrency
// namespace.hCircuit.name.err_count.err_ErrTimeout
// namespace.hCircuit.name.err_count.err_NonHystrixErr
// Timing:
// namespace.hCircuit.name.get_duration.result_ok
// namespace.hCircuit.name.get_duration.result_err
func RegisterCircuitBreakerHandlerMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_CIRCUITBREAKER,
		name, gentle.MX_HANDLER_OB_HANDLE}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_CIRCUITBREAKER, name)
	if gentle.GetObservation(key) == nil {
		ob := &timingObservationImpl{
			count:  statter.NewSubStatter(prefix + ".get_count"),
			timing: statter.NewSubStatter(prefix + ".get_duration"),
		}
		gentle.RegisterObservation(key, ob)
	}
	key = &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_CIRCUITBREAKER,
		name,
		gentle.MX_HANDLER_CIRCUITBREAKER_CNT_HXERR}
	if gentle.GetCounter(key) == nil {
		counter := &counterImpl{
			count: statter.NewSubStatter(prefix + ".err_count"),
		}
		gentle.RegisterCounter(key, counter)
	}
}
