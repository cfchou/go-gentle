package gentle

import "sync"

const (
	// Observation supported by all Streams, it observes the time spent for
	// Get() with label "result" of possible values "ok" and "err"
	MX_STREAM_OB_GET = "get"

	// Observation supported by all Handlers, it observes the time spent for
	// Handle() with label "result" of possible values "ok" and "err"
	MX_HANDLER_OB_HANDLE = "handle"

	// Observation supported by RetryStreams, it observes the total number
	// of tries with label "result" of possible values "ok" and "err"
	MX_STREAM_RETRY_OB_TRY = "try"

	// Observation supported by RetryHandler, it observes the total number
	// of tries with label "result" of possible values "ok" and "err"
	MX_HANDLER_RETRY_OB_TRY = "try"

	// Counter supported by CircuitBreakerStream, it counts the number of
	// errors with label "err" of possible values of "ErrCircuitOpen",
	// "ErrMaxConcurrency", "ErrTimeout", "NonHystrixErr":
	MX_STREAM_CIRCUITBREAKER_CNT_HXERR = "hxerr"

	// Counter supported by CircuitBreakerHandler, it counts the number of
	// errors with label "err" of possible values of "ErrCircuitOpen",
	// "ErrMaxConcurrency", "ErrTimeout", "NonHystrixErr":
	MX_HANDLER_CIRCUITBREAKER_CNT_HXERR = "hxerr"
)

var gentleMetrics = &metricRegistry{
	counters:     make(map[RegistryKey]Counter),
	observations: make(map[RegistryKey]Observation),
	lock:         &sync.RWMutex{},
}

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

type RegistryKey struct {
	Namespace, Mixin, Name, Mx string
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
	counters     map[RegistryKey]Counter
	observations map[RegistryKey]Observation
	lock         *sync.RWMutex
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

func (r *metricRegistry) RegisterObservation(key *RegistryKey, observation Observation) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.observations[*key] = observation
}

func (r *metricRegistry) UnRegisterObservation(key *RegistryKey) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.counters, *key)
}

func (r *metricRegistry) GetObservation(key *RegistryKey) Observation {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.observations[*key]
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
