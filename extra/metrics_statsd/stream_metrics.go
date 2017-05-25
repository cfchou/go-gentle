package metrics_statsd

import (
	"fmt"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/cfchou/go-gentle/gentle"
)

// We define statsd metrics for gentle components in this format:
// <namespace>.<mixin_name>.<name>.<metrics_op>.<label>
//
// Unlike prometheus, statsd doesn't support multi-dimensional metrics.
// Therefore, we squash a pair of key and value into a <label>(in the form of
// key_value) in the name of metrics.

// Counter:
// namespace.sRate.name.get.result_ok
// namespace.sRate.name.get.result_err
// Timing:
// namespace.sRate.name.get.result_ok
// namespace.sRate.name.get.result_err
func NewRateLimitedStreamOpts(statter statsd.SubStatter, namespace, name string, limiter gentle.RateLimit) *gentle.RateLimitedStreamOpts {
	opts := gentle.NewRateLimitedStreamOpts(namespace, name, limiter)
	prefix := fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_RATELIMITED, name, gentle.MX_STREAM_GET)
	opts.MetricGet = &timingImpl{
			count:  statter.NewSubStatter(prefix),
			timing: statter.NewSubStatter(prefix),
	}
	return opts
}

// Counter:
// namespace.sRetry.name.get.result_ok
// namespace.sRetry.name.get.result_err
// namespace.sRetry.name.try.result_ok
// namespace.sRetry.name.try.result_err
// Timing:
// namespace.sRetry.name.get.result_ok
// namespace.sRetry.name.get.result_err
func NewRetryStreamOpts(statter statsd.SubStatter, namespace, name string,
	backoff gentle.BackOff, tryBuckets []float64) *gentle.RetryStreamOpts {

	opts := gentle.NewRetryStreamOpts(namespace, name, backoff)
	prefix := fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_RETRY, name, gentle.MX_STREAM_GET)

	opts.MetricGet = &timingImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
	}

	prefix = fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_RETRY, name, gentle.MX_STREAM_RETRY_TRY)
	opts.MetricTryNum = &counterImpl{
		count: statter.NewSubStatter(prefix),
	}
	return opts
}

// Counter:
// namespace.sBulk.name.get.result_ok
// namespace.sBulk.name.get.result_err
// Timing:
// namespace.sBulk.name.get.result_ok
// namespace.sBulk.name.get.result_err
func NewBulkStreamOpts(statter statsd.SubStatter, namespace, name string,
	max_concurrency int) *gentle.BulkheadStreamOpts {

	opts := gentle.NewBulkheadStreamOpts(namespace, name, max_concurrency)
	prefix := fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_BULKHEAD, name, gentle.MX_STREAM_GET)
	opts.MetricGet = &timingImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
	}
	return opts
}

// Counter:
// namespace.sCircuit.name.get.result_ok
// namespace.sCircuit.name.get.result_err
// namespace.sCircuit.name.herr.err_ErrCircuitOpen
// namespace.sCircuit.name.herr.err_ErrMaxConcurrency
// namespace.sCircuit.name.herr.err_ErrTimeout
// namespace.sCircuit.name.herr.err_NonHystrixErr
// Timing:
// namespace.sCircuit.name.get.result_ok
// namespace.sCircuit.name.get.result_err
func NewCircuitBreakerStreamOpts(statter statsd.SubStatter, namespace, name string,
	circuit string) *gentle.CircuitBreakerStreamOpts {

	opts := gentle.NewCircuitBreakerStreamOpts(namespace, name, circuit)
	prefix := fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_CIRCUITBREAKER, name, gentle.MX_STREAM_GET)
	opts.MetricGet = &timingImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
	}

	prefix = fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_CIRCUITBREAKER, name,
		gentle.MX_HANDLER_CIRCUITBREAKER_HXERR)
	opts.MetricCbErr = &counterImpl{
		count: statter.NewSubStatter(prefix),
	}
	return opts
}

// Counter:
// namespace.sChan.name.get.result_ok
// namespace.sChan.name.get.result_err
// Timing:
// namespace.sChan.name.get.result_ok
// namespace.sChan.name.get.result_err
func NewChannelStreamOpts(statter statsd.SubStatter, namespace, name string,
	channel <-chan interface{}) *gentle.ChannelStreamOpts {

	opts := gentle.NewChannelStreamOpts(namespace, name, channel)
	prefix := fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_CHANNEL, name, gentle.MX_STREAM_GET)
	opts.MetricGet = &timingImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
	}
	return opts
}

// Counter:
// namespace.sHan.name.get.result_ok
// namespace.sHan.name.get.result_err
// Timing:
// namespace.sHan.name.get.result_ok
// namespace.sHan.name.get.result_err
func NewHandlerStreamOpts(statter statsd.SubStatter, namespace, name string) *gentle.HandlerStreamOpts {
	opts := gentle.NewHandlerStreamOpts(namespace, name)
	prefix := fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_HANDLED, name, gentle.MX_STREAM_GET)
	opts.MetricGet = &timingImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
	}
	return opts
}

// Counter:
// namespace.sTrans.name.get.result_ok
// namespace.sTrans.name.get.result_err
// Timing:
// namespace.sTrans.name.get.result_ok
// namespace.sTrans.name.get.result_err
func NewTransformStreamOpts(statter statsd.SubStatter, namespace, name string,
	transFunc func(gentle.Message, error) (gentle.Message, error)) *gentle.TransformStreamOpts {

	opts := gentle.NewTransformStreamOpts(namespace, name, transFunc)
	prefix := fmt.Sprintf("%s.%s.%s.%s", namespace,
		gentle.MIXIN_STREAM_TRANS, name, gentle.MX_STREAM_GET)
	opts.MetricGet = &timingImpl{
		count:  statter.NewSubStatter(prefix),
		timing: statter.NewSubStatter(prefix),
	}
	return opts
}

