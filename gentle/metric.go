package gentle

import (
	"sync"
	"errors"
)

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

var (
	ErrMxNotFound = errors.New("Metric Not found")
	gentleMetrics = &metricRegistry{
		metrics: make(map[RegistryKey]Metric),
		lock:         &sync.RWMutex{},
	}
)

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

// Registration helpers. Not thread-safe so synchronization has be done by
// application if needed.
func RegisterObservation(key *RegistryKey, observation Observation) {
	gentleMetrics.RegisterMetric(key, observation)
}

// Registration helpers. Not thread-safe so synchronization has be done by
// application if needed.
func UnRegisterObservation(key *RegistryKey) {
	gentleMetrics.UnRegisterMetric(key)
}

// Registration helpers. Not thread-safe so synchronization has be done by
// application if needed.
func GetObservation(key *RegistryKey) (Observation, error) {
	mx := gentleMetrics.GetMetric(key)
	if ob, ok := mx.(Observation); ok {
		return ob, nil
	}
	return nil, ErrMxNotFound
}

type metricRegistry struct {
	metrics map[RegistryKey]Metric
	lock         *sync.RWMutex
}

func (r *metricRegistry) RegisterMetric(key *RegistryKey, mx Metric) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.metrics[*key] = mx
}

func (r *metricRegistry) UnRegisterMetric(key *RegistryKey) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.metrics, *key)
}

func (r *metricRegistry) GetMetric(key *RegistryKey) Metric {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.metrics[*key]
}

// A do-nothing Metric
type NoOpMetric struct{}

func (m *NoOpMetric) Observe(value float64, labels map[string]string) {}

var noop = &NoOpMetric{}

func NoOpObservationIfNonRegistered(key *RegistryKey) Observation {
	if ob, err := GetObservation(key); err == nil {
		return ob
	}
	return noop
}
