package gentle


// A do-nothing Metric
type DummyMetric struct {
}

func (m *DummyMetric) BaseName() string {
	return ""
}

// Counter/Gauge
func (m *DummyMetric) Add(delta float64) {}
func (m *DummyMetric) AddWithLabels(delta float64, kv map[string]string) {}

// Gauge
func (m *DummyMetric) Set(value float64) {}
func (m *DummyMetric) SetWithLabels(value float64, kv map[string]string) {}

// Histogram
func (m *DummyMetric) Observe(value float64) {}
func (m *DummyMetric) ObserveWithLabels(value float64, kv map[string]string) {}


type DummyMetricRegistry struct {
	metric *DummyMetric
}

func (r *DummyMetricRegistry) RegisterCounter(name string, help string, kv map[string]string) Counter {
	return r.metric
}

func (r *DummyMetricRegistry) RegisterGauge(name string, help string, kv map[string]string) Gauge {
	return r.metric
}

func (r *DummyMetricRegistry) RegisterHistogram(name string, help string, kv map[string]string) Histogram {
	return r.metric
}
