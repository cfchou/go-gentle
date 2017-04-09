package metrics_statsd

import (
	"fmt"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/cfchou/go-gentle/gentle"
)

// Counter:
// namespace.hRate.name.get.result_ok
// namespace.hRate.name.get.result_err
// Timing:
// namespace.hRate.name.get.result_ok
// namespace.hRate.name.get.result_err
func RegisterRateLimitedHandlerMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_RATELIMITED,
		name, gentle.MX_HANDLER_HANDLE}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_RATELIMITED, name)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + "get"),
		timing: statter.NewSubStatter(prefix + "get"),
	})
}

// Counter:
// namespace.hRetry.name.get.result_ok
// namespace.hRetry.name.get.result_err
// namespace.hRetry.name.try.result_ok
// namespace.hRetry.name.try.result_err
// Timing:
// namespace.hRetry.name.get.result_ok
// namespace.hRetry.name.get.result_err
func RegisterRetryHandlerMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_RETRY,
		name, gentle.MX_HANDLER_HANDLE}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_RETRY, name)
	if _, err := gentle.GetObservation(key); err == nil {
		gentle.RegisterObservation(key, &timingObservationImpl{
			count:  statter.NewSubStatter(prefix + "get"),
			timing: statter.NewSubStatter(prefix + "get"),
		})
	}
	key = &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_RETRY,
		name, gentle.MX_HANDLER_RETRY_TRY}

	if _, err := gentle.GetObservation(key); err == nil {
		gentle.RegisterObservation(key, &counterImpl{
			count: statter.NewSubStatter(prefix + ".try"),
		})
	}
}

// Counter:
// namespace.hBulk.name.get.result_ok
// namespace.hBulk.name.get.result_err
// Timing:
// namespace.hBulk.name.get.result_ok
// namespace.hBulk.name.get.result_err
func RegisterBulkheadHandlerMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_BULKHEAD,
		name, gentle.MX_HANDLER_HANDLE}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_BULKHEAD, name)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix + "get"),
		timing: statter.NewSubStatter(prefix + "get"),
	})
}

// Counter:
// namespace.hCircuit.name.get.result_ok
// namespace.hCircuit.name.get.result_err
// namespace.hCircuit.name.err.err_ErrCircuitOpen
// namespace.hCircuit.name.err.err_ErrMaxConcurrency
// namespace.hCircuit.name.err.err_ErrTimeout
// namespace.hCircuit.name.err.err_NonHystrixErr
// Timing:
// namespace.hCircuit.name.get.result_ok
// namespace.hCircuit.name.get.result_err
func RegisterCircuitBreakerHandlerMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_CIRCUITBREAKER,
		name, gentle.MX_HANDLER_HANDLE}
	prefix := fmt.Sprintf("%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_CIRCUITBREAKER, name)
	if _, err := gentle.GetObservation(key); err == nil {
		gentle.RegisterObservation(key, &timingObservationImpl{
			count:  statter.NewSubStatter(prefix + "get"),
			timing: statter.NewSubStatter(prefix + "get"),
		})
	}
	key = &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_CIRCUITBREAKER,
		name,
		gentle.MX_HANDLER_CIRCUITBREAKER_HXERR}
	if _, err := gentle.GetObservation(key); err == nil {
		gentle.RegisterObservation(key, &counterImpl{
			count: statter.NewSubStatter(prefix + ".err"),
		})
	}
}
