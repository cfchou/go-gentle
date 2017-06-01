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
func NewRateLimitedHandlerOpts(statter statsd.SubStatter, namespace, name string, limiter gentle.RateLimit) *gentle.RateLimitedHandlerOpts {
	opts := gentle.NewRateLimitedHandlerOpts(namespace, name, limiter)
	prefix := fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_RATELIMITED, name, gentle.MX_HANDLER_HANDLE)
	opts.MetricHandle = &timingImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
	}
	return opts
}

// Counter:
// namespace.hRetry.name.handle.result_ok
// namespace.hRetry.name.handle.result_err
// namespace.hRetry.name.try.result_ok
// namespace.hRetry.name.try.result_err
// Timing:
// namespace.hRetry.name.handle.result_ok
// namespace.hRetry.name.handle.result_err
func NewRetryHandlerOpts(statter statsd.SubStatter, namespace, name string,
	backoff gentle.BackOff, tryBuckets []float64) *gentle.RetryHandlerOpts {

	opts := gentle.NewRetryHandlerOpts(namespace, name, backoff)
	prefix := fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_RETRY, name, gentle.MX_HANDLER_HANDLE)
	opts.MetricHandle = &timingImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
	}
	prefix = fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_RETRY, name, gentle.MX_HANDLER_RETRY_TRY)
	opts.MetricTryNum = &counterImpl{
		count: statter.NewSubStatter(prefix),
	}
	return opts
}

// Counter:
// namespace.hBulk.name.handle.result_ok
// namespace.hBulk.name.handle.result_err
// Timing:
// namespace.hBulk.name.handle.result_ok
// namespace.hBulk.name.handle.result_err
func NewBulkheadHandlerOpts(statter statsd.SubStatter, namespace, name string,
	max_concurrency int) *gentle.BulkheadHandlerOpts {

	opts := gentle.NewBulkheadHandlerOpts(namespace, name, max_concurrency)
	prefix := fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_BULKHEAD, name, gentle.MX_HANDLER_HANDLE)
	opts.MetricHandle = &timingImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
	}
	return opts
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
func NewCircuitBreakerHandlerOpts(statter statsd.SubStatter, namespace, name,
	circuit string) *gentle.CircuitBreakerHandlerOpts {

	opts := gentle.NewCircuitBreakerHandlerOpts(namespace, name, circuit)
	prefix := fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_CIRCUITBREAKER, name,
		gentle.MX_HANDLER_HANDLE)
	opts.MetricHandle = &timingImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
	}
	prefix = fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_CIRCUITBREAKER, name,
		gentle.MX_HANDLER_CIRCUITBREAKER_HXERR)
	opts.MetricCbErr = &counterImpl{
		count: statter.NewSubStatter(prefix),
	}
	return opts
}

// Counter:
// namespace.hFb.name.handle.result_ok
// namespace.hFb.name.handle.result_err
// Timing:
// namespace.hFb.name.handle.result_ok
// namespace.hFb.name.handle.result_err
func NewFallbackHandlerOpts(statter statsd.SubStatter, namespace, name string,
	fallbackFunc func(gentle.Message, error) (gentle.Message, error)) *gentle.FallbackHandlerOpts {

	opts := gentle.NewFallbackHandlerOpts(namespace, name, fallbackFunc)
	prefix := fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_HANDLER_FB, name, gentle.MX_HANDLER_HANDLE)
	opts.MetricHandle = &timingImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
	}
	return opts
}
