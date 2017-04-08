package gentle

import "sync"

const (
	// Observation supported by all Stream.Get(), it observes the time spent
	// with the label "result" of possible values "ok" or "err"
	MX_STREAM_GET = "get"

	// Observation supported by all Handler.Handle(), it observes the time
	// spent with the label "result" of possible values "ok" or "err"
	MX_HANDLER_HANDLE = "handle"

	// Observation supported by RetryStreams.Get(), it observes the total
	// number tries with the label "result" of possible values "ok" or "err"
	MX_STREAM_RETRY_TRY = "try"

	// Observation supported by RetryHandler.Handle(), it observes the total
	// number of tries with the label "result" of possible values "ok" or
	// "err"
	MX_HANDLER_RETRY_TRY = "try"

	// Observation supported by CircuitBreakerStream.Get(), when an error is
	// met, it observes 1 with the label "err" of possible values of
	// "ErrCircuitOpen", "ErrMaxConcurrency", "ErrTimeout" or
	// "NonHystrixErr"
	MX_STREAM_CIRCUITBREAKER_HXERR = "hxerr"

	// Observation supported by CircuitBreakerHandler.Handle(), when an
	// error is met, it observes 1 with the label "err" of possible values
	// of "ErrCircuitOpen", "ErrMaxConcurrency", "ErrTimeout" or
	// "NonHystrixErr"
	MX_HANDLER_CIRCUITBREAKER_HXERR = "hxerr"
)

var gentleMetrics = &metricRegistry{
	observations: make(map[RegistryKey]Observation),
	lock:         &sync.RWMutex{},
}

// Metric
type Metric interface{}

// Instead of commonly used Counter/Gauge/Timer/Histogram/Percentile,
// Observation is a general interface that doesn't limit the implementation. An
// implementation can be whatever meaningful.
type Observation interface {
	Metric
	Observe(value float64, labels map[string]string)
}

type RegistryKey struct {
	Namespace, Mixin, Name, Mx string
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
	observations map[RegistryKey]Observation
	lock         *sync.RWMutex
}

func (r *metricRegistry) RegisterObservation(key *RegistryKey, observation Observation) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.observations[*key] = observation
}

func (r *metricRegistry) UnRegisterObservation(key *RegistryKey) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.observations, *key)
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

func dummyObservationIfNonRegistered(key *RegistryKey) Observation {
	m := gentleMetrics.GetObservation(key)
	if m != nil {
		return m
	}
	return dummy
}
