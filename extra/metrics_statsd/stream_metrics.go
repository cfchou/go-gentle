package metrics_statsd

import (
	"fmt"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/cfchou/go-gentle/gentle"
)

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
	prefix := fmt.Sprintf("%s.%s.%s.get", namespace,
		gentle.MIXIN_STREAM_RATELIMITED, name)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
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
			count:  statter.NewSubStatter(prefix + ".get"),
			timing: statter.NewSubStatter(prefix + ".get"),
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
	prefix := fmt.Sprintf("%s.%s.%s.get", namespace,
		gentle.MIXIN_STREAM_BULKHEAD, name)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
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
			count:  statter.NewSubStatter(prefix + ".get"),
			timing: statter.NewSubStatter(prefix + ".get"),
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
	prefix := fmt.Sprintf("%s.%s.%s.get", namespace,
		gentle.MIXIN_STREAM_CHANNEL, name)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
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
	prefix := fmt.Sprintf("%s.%s.%s.get", namespace,
		gentle.MIXIN_STREAM_CONCURRENTFETCH, name)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
	})
}

// Counter:
// namespace.sHan.name.get.result_ok
// namespace.sHan.name.get.result_err
// Timing:
// namespace.sHan.name.get.result_ok
// namespace.sHan.name.get.result_err
func RegisterHandlerStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_HANDLED,
		name, gentle.MX_STREAM_GET}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s.get", namespace,
		gentle.MIXIN_STREAM_HANDLED, name)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
	})
}

// Counter:
// namespace.sTrans.name.get.result_ok
// namespace.sTrans.name.get.result_err
// Timing:
// namespace.sTrans.name.get.result_ok
// namespace.sTrans.name.get.result_err
func RegisterTransformStreamMetrics(statter statsd.SubStatter, namespace, name string) {
	key := &gentle.RegistryKey{namespace,
		gentle.MIXIN_STREAM_TRANS,
		name, gentle.MX_STREAM_GET}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
	prefix := fmt.Sprintf("%s.%s.%s.get", namespace,
		gentle.MIXIN_STREAM_TRANS, name)
	gentle.RegisterObservation(key, &timingObservationImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
	})
}
