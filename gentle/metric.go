package gentle

import "sync"

var gentleMetrics = &metricRegistry{}

// Metric
type Metric interface{}

type Counter interface {
	Metric
	Add(delta float64, labels map[string]string)
}

// Instead of commonly used Gauge/Timer/Histogram/Percentile, I feel
// Observation is a better name that doesn't limit the implementation. An
// implementation can actually be a Gauge/Timer/Histogram/Percentile or
// whatever.
type Observation interface {
	Metric
	Observe(value float64, labels map[string]string)
}

type MetricDesc interface {
	// Return []string that each of which can be used as a SubKey.
	SupportedCounters(mixin string) []string
	// Return []string that each of which can be used as a SubKey.
	SupportedObservations(mixin string) []string
	// Return map[string]string that can be used in Counter's Add
	// or Observation's Observe.
	SupportedLabels(mixin string, subkey string) []string
}

type RegistryKey struct {
	Namespace, Mixin, Name, SubKey string
}

func RegisterCounter(key *RegistryKey, counter Counter) {
	gentleMetrics.RegisterCounter(key, counter)
}

func UnRegisterCounter(key *RegistryKey) {
	gentleMetrics.UnRegisterCounter(key)
}

func GetCounter(key *RegistryKey) Counter {
	return gentleMetrics.GetCounter(key)
}

func RegisterObservation(key *RegistryKey, observation Observation) {
	gentleMetrics.RegisterObservation(key, observation)
}

func UnRegisterObservation(key *RegistryKey) {
	gentleMetrics.UnRegisterObservation(key)
}
func GetObservation(key *RegistryKey) Observation {
	return gentleMetrics.GetObservation(key)
}

type metricRegistry struct {
	counters map[RegistryKey]Counter
	timers   map[RegistryKey]Observation
	lock     sync.RWMutex
}

func (r *metricRegistry) RegisterCounter(key *RegistryKey, counter Counter) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.counters[*key] = counter
}

func (r *metricRegistry) UnRegisterCounter(key *RegistryKey) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.counters, *key)
}

func (r *metricRegistry) GetCounter(key *RegistryKey) Counter {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.counters[*key]
}

func (r *metricRegistry) RegisterObservation(key *RegistryKey, timer Observation) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.timers[*key] = timer
}

func (r *metricRegistry) UnRegisterObservation(key *RegistryKey) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.counters, *key)
}

func (r *metricRegistry) GetObservation(key *RegistryKey) Observation {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.timers[*key]
}

// A do-nothing Metric
type dummyMetric struct{}

func (m *dummyMetric) Observe(value float64, labels map[string]string) {}
func (m *dummyMetric) Add(delta float64, labels map[string]string)     {}

var dummy = &dummyMetric{}

func dummyCounterIfNonRegistered(key *RegistryKey) Counter {
	m := gentleMetrics.GetCounter(key)
	if m != nil {
		return m
	}
	return dummy
}

func dummyObservationIfNonRegistered(key *RegistryKey) Observation {
	m := gentleMetrics.GetObservation(key)
	if m != nil {
		return m
	}
	return dummy
}
