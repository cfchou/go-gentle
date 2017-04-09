package metrics_statsd

import "github.com/cactus/go-statsd-client/statsd"

// Provide registration APIs for applications don't bother to create a statsd
// client themselves.
type StatsdMetrics struct {
	Client statsd.Client
	statter statsd.SubStatter
}

func NewStatsdMetrics(addr, prefix string) (*StatsdMetrics, error) {
	client, err := statsd.NewClient(addr, prefix)
	if err != nil {
		return nil, err
	}
	return &StatsdMetrics{
		Client: client.(statsd.Client),
		statter: client.NewSubStatter(""),
	}, nil
}

func (m *StatsdMetrics) RegisterRateLimitedStreamMetrics(namespace, name string) {
	RegisterRateLimitedStreamMetrics(m.statter, namespace, name)
}

func (m *StatsdMetrics) RegisterRetryStreamMetrics(namespace, name string) {
	RegisterRetryStreamMetrics(m.statter, namespace, name)
}

func (m *StatsdMetrics) RegisterBulkheadStreamMetrics(namespace, name string) {
	RegisterBulkheadStreamMetrics(m.statter, namespace, name)
}

func (m *StatsdMetrics) RegisterCircuitBreakerStreamMetrics(namespace, name string) {
	RegisterCircuitBreakerStreamMetrics(m.statter, namespace, name)
}

func (m *StatsdMetrics) RegisterChannelStreamMetrics(namespace, name string) {
	RegisterChannelStreamMetrics(m.statter, namespace, name)
}

func (m *StatsdMetrics) RegisterConcurrentFetchStreamMetrics(namespace, name string) {
	RegisterConcurrentFetchStreamMetrics(m.statter, namespace, name)
}

func (m *StatsdMetrics) RegisterMappedStreamMetrics(namespace, name string) {
	RegisterMappedStreamMetrics(m.statter, namespace, name)
}

func (m *StatsdMetrics) RegisterRateLimitedHandlerMetrics(namespace, name string) {
	RegisterRateLimitedHandlerMetrics(m.statter, namespace, name)
}

func (m *StatsdMetrics) RegisterRetryHandlerMetrics(namespace, name string) {
	RegisterRetryHandlerMetrics(m.statter, namespace, name)
}

func (m *StatsdMetrics) RegisterBulkheadHandlerMetrics(namespace, name string) {
	RegisterBulkheadHandlerMetrics(m.statter, namespace, name)
}

func (m *StatsdMetrics) RegisterCircuitBreakerHandlerMetrics(namespace, name string) {
	RegisterCircuitBreakerHandlerMetrics(m.statter, namespace, name)
}

// A statsd timing maintains a counter too but it doesn't get flushed every
// interval. Therefore we explicitly set up a statsd counter.
// More info:
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

func (p *counterImpl) Observe(value float64, labels map[string]string) {
	for k, v := range labels {
		suffix := k + "_" + v
		p.count.Inc(suffix, int64(value), 1.0)
	}
}

