package gentle

import "sync"

var GentleMetrics = &metricRegistry{}

type RegistryKey struct {
	Namespace, Mixin, Name, SubKey string
}

type metricRegistry struct {
	registry map[RegistryKey]Metric
	lock sync.RWMutex
}

func (r *metricRegistry) RegisterMetric(key RegistryKey, metric Metric) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.registry[key] = metric
}

func (r *metricRegistry) UnRegisterMetric(key RegistryKey) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.registry, key)
}

func (r *metricRegistry) GetMetric(key RegistryKey) Metric {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.registry[key]
}

// A do-nothing Metric
type dummyMetric struct {}

func (m *dummyMetric) Observe(value float64, labels map[string]string)

var dummy = &dummyMetric{}

func dummyMetricIfNonRegistered(key RegistryKey) Metric {
	m := GentleMetrics.GetMetric(key)
	if m != nil {
		return m
	}
	return dummy
}

