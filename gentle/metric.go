package gentle

type Metric interface {
	ObserveOk(value float64)
	ObserveErr(value float64)
}

type RetryMetric interface {
	ObserveOk(value float64, retry int)
	ObserveErr(value float64, retry int)
}

type CbMetric interface {
	ObserveOk(value float64)
	ObserveErr(value float64, err error)
}

type noOpMetric struct{}

func (m *noOpMetric) ObserveOk(value float64)  {}
func (m *noOpMetric) ObserveErr(value float64) {}

type noOpRetryMetric struct{}

func (m *noOpRetryMetric) ObserveOk(value float64, retry int)  {}
func (m *noOpRetryMetric) ObserveErr(value float64, retry int) {}

type noOpCbMetric struct{}

func (m *noOpCbMetric) ObserveOk(value float64)             {}
func (m *noOpCbMetric) ObserveErr(value float64, err error) {}

var (
	noopMetric      = &noOpMetric{}
	noopRetryMetric = &noOpRetryMetric{}
	noopCbMetric    = &noOpCbMetric{}
)
