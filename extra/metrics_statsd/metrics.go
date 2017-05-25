package metrics_statsd

import (
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/cfchou/go-gentle/gentle"
)

// Provide registration APIs for applications don't bother to create a statsd
// client themselves.
type StatsdMetrics struct {
	Client  *statsd.Client
	statter statsd.SubStatter
}

func NewStatsdMetrics(addr, prefix string) (*StatsdMetrics, error) {
	client, err := statsd.NewClient(addr, prefix)
	if err != nil {
		return nil, err
	}
	return &StatsdMetrics{
		Client:  client.(*statsd.Client),
		statter: client.NewSubStatter(""),
	}, nil
}

func (m *StatsdMetrics) NewRateLimitedStreamOpts(namespace, name string,
	limiter gentle.RateLimit) *gentle.RateLimitedStreamOpts {
	return NewRateLimitedStreamOpts(m.statter, namespace, name, limiter)
}

func (m *StatsdMetrics) NewRetryStreamOpts(namespace, name string,
	backoff gentle.BackOff, tryBuckets []float64) *gentle.RetryStreamOpts {
	return NewRetryStreamOpts(m.statter, namespace, name, backoff,
		tryBuckets)
}

func (m *StatsdMetrics) NewBulkheadStreamOpts(namespace, name string,
	max_concurrency int) *gentle.BulkheadHandlerOpts {
	return NewBulkheadHandlerOpts(m.statter, namespace, name,
		max_concurrency)
}

func (m *StatsdMetrics) NewCircuitBreakerStreamOpts(namespace, name, circuit string) *gentle.CircuitBreakerStreamOpts {
	return NewCircuitBreakerStreamOpts(m.statter, namespace, name, circuit)
}

func (m *StatsdMetrics) NewChannelStreamOpts(namespace, name string,
	channel <-chan interface{}) *gentle.ChannelStreamOpts {
	return NewChannelStreamOpts(m.statter, namespace, name, channel)
}

func (m *StatsdMetrics) NewHandlerStreamOpts(namespace, name string) *gentle.HandlerStreamOpts {
	return NewHandlerStreamOpts(m.statter, namespace, name)
}

func (m *StatsdMetrics) NewTransformStreamOpts(namespace, name string,
	transFunc func(gentle.Message, error) (gentle.Message, error)) *gentle.TransformStreamOpts {
	return NewTransformStreamOpts(m.statter, namespace, name, transFunc)
}

func (m *StatsdMetrics) NewRateLimitedHandlerOpts(namespace, name string,
	limiter gentle.RateLimit) *gentle.RateLimitedHandlerOpts {
	return NewRateLimitedHandlerOpts(m.statter, namespace, name, limiter)
}

func (m *StatsdMetrics) NewRetryHandlerOpts(namespace, name string,
	backoff gentle.BackOff, tryBuckets []float64) *gentle.RetryHandlerOpts {
	return NewRetryHandlerOpts(m.statter, namespace, name, backoff,
		tryBuckets)
}

func (m *StatsdMetrics) NewBulkheadHandlerOpts(namespace, name string,
	max_concurrency int) *gentle.BulkheadHandlerOpts {
	return NewBulkheadHandlerOpts(m.statter, namespace, name,
		max_concurrency)
}

func (m *StatsdMetrics) NewCircuitBreakerHandlerOpts(namespace, name, circuit string) *gentle.CircuitBreakerHandlerOpts {
	return NewCircuitBreakerHandlerOpts(m.statter, namespace, name, circuit)
}

func (m *StatsdMetrics) NewTransformHandlerOpts(namespace, name string,
	transFunc func(gentle.Message, error) (gentle.Message, error)) *gentle.TransformHandlerOpts {
	return NewTransformHandlerOpts(m.statter, namespace, name, transFunc)
}

// A statsd native timing maintains a counter too, but it doesn't get flushed
// every interval. Therefore we explicitly set up a statsd counter.
// More info:
// https://github.com/etsy/statsd/issues/22
type timingImpl struct {
	count  statsd.SubStatter
	timing statsd.SubStatter
}

func (p *timingImpl) Observe(value float64, labels map[string]string) {
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
