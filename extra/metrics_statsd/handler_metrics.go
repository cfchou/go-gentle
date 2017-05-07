package metrics_statsd

import (
	"fmt"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/cfchou/go-gentle/gentle"
)

// Counter:
// namespace.hRate.name.handle.result_ok
// namespace.hRate.name.handle.result_err
// Timing:
// namespace.hRate.name.handle.result_ok
// namespace.hRate.name.handle.result_err
func RegisterRateLimitedHandlerMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_RATELIMITED,
		name, gentle.MX_HANDLER_HANDLE}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_RATELIMITED, name, gentle.MX_HANDLER_HANDLE)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
	})
}

// Counter:
// namespace.hRetry.name.handle.result_ok
// namespace.hRetry.name.handle.result_err
// namespace.hRetry.name.try.result_ok
// namespace.hRetry.name.try.result_err
// Timing:
// namespace.hRetry.name.handle.result_ok
// namespace.hRetry.name.handle.result_err
func RegisterRetryHandlerMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_RETRY,
		name, gentle.MX_HANDLER_HANDLE}
	prefix := fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_RETRY, name, gentle.MX_HANDLER_HANDLE)
	if _, err := gentle.GetObservation(key); err == nil {
		gentle.RegisterObservation(key, &timingObservationImpl{
			count:  statter.NewSubStatter(prefix),
			timing: statter.NewSubStatter(prefix),
		})
	}

	key = &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_RETRY,
		name, gentle.MX_HANDLER_RETRY_TRY}
	prefix = fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_RETRY, name, gentle.MX_HANDLER_RETRY_TRY)

	if _, err := gentle.GetObservation(key); err == nil {
		gentle.RegisterObservation(key, &counterImpl{
			count: statter.NewSubStatter(prefix),
		})
	}
}

// Counter:
// namespace.hBulk.name.handle.result_ok
// namespace.hBulk.name.handle.result_err
// Timing:
// namespace.hBulk.name.handle.result_ok
// namespace.hBulk.name.handle.result_err
func RegisterBulkheadHandlerMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_BULKHEAD,
		name, gentle.MX_HANDLER_HANDLE}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_BULKHEAD, name, gentle.MX_HANDLER_HANDLE)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
	})
}

// Counter:
// namespace.hCircuit.name.handle.result_ok
// namespace.hCircuit.name.handle.result_err
// namespace.hCircuit.name.herr.err_ErrCircuitOpen
// namespace.hCircuit.name.herr.err_ErrMaxConcurrency
// namespace.hCircuit.name.herr.err_ErrTimeout
// namespace.hCircuit.name.herr.err_NonHystrixErr
// Timing:
// namespace.hCircuit.name.handle.result_ok
// namespace.hCircuit.name.handle.result_err
func RegisterCircuitBreakerHandlerMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_CIRCUITBREAKER,
		name, gentle.MX_HANDLER_HANDLE}
	prefix := fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_CIRCUITBREAKER, name,
		gentle.MX_HANDLER_HANDLE)
	if _, err := gentle.GetObservation(key); err == nil {
		gentle.RegisterObservation(key, &timingObservationImpl{
			count:  statter.NewSubStatter(prefix),
			timing: statter.NewSubStatter(prefix),
		})
	}
	key = &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_CIRCUITBREAKER,
		name,
		gentle.MX_HANDLER_CIRCUITBREAKER_HXERR}
	prefix = fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_CIRCUITBREAKER, name,
		gentle.MX_HANDLER_CIRCUITBREAKER_HXERR)
	if _, err := gentle.GetObservation(key); err == nil {
		gentle.RegisterObservation(key, &counterImpl{
			count: statter.NewSubStatter(prefix),
		})
	}
}

// Counter:
// namespace.hTrans.name.handle.result_ok
// namespace.hTrans.name.handle.result_err
// Timing:
// namespace.hTrans.name.handle.result_ok
// namespace.hTrans.name.handle.result_err
func RegisterTransformHandlerMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_HANDLER_TRANS,
		name, gentle.MX_HANDLER_HANDLE}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_TRANS, name, gentle.MX_HANDLER_HANDLE)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
	})
}
