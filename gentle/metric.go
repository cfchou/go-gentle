package gentle

// Metric is an interface for collecting metrics by
// RateLimitStream/RateLimitHandler/BulkheadStream/BulkheadHandler
type Metric interface {
	// Successful Stream.Get()/Handler.Handle() with timespan in seconds
	ObserveOk(timespan float64)
	// Failed Stream.Get()/Handler.Handle() with timespan in seconds
	ObserveErr(timespan float64)
}

// RetryMetric is an interface for collecting metrics by RetryStream/RetryHandler
type RetryMetric interface {
	// Successful Stream.Get()/Handler.Handle() with timespan in seconds and the number of retries
	ObserveOk(timespan float64, retry int)
	// Failed Stream.Get()/Handler.Handle()  with timespan in seconds and the number of retries
	ObserveErr(timespan float64, retry int)
}

// CbMetric is an interface for collecting metrics by CircuitBreakerStream/CircuitBreakerHandler
type CbMetric interface {
	// Successful Stream.Get()/Handler.Handle() with timespan in seconds
	ObserveOk(timespan float64)
	// Failed Stream.Get()/Handler.Handle() with timespan in seconds and error.
	ObserveErr(timespan float64, err error)
}

type noOpMetric struct{}

func (m *noOpMetric) ObserveOk(timespan float64)  {}
func (m *noOpMetric) ObserveErr(timespan float64) {}

type noOpRetryMetric struct{}

func (m *noOpRetryMetric) ObserveOk(timespan float64, retry int)  {}
func (m *noOpRetryMetric) ObserveErr(timespan float64, retry int) {}

type noOpCbMetric struct{}

func (m *noOpCbMetric) ObserveOk(timespan float64)             {}
func (m *noOpCbMetric) ObserveErr(timespan float64, err error) {}

var (
	noopMetric      = &noOpMetric{}
	noopRetryMetric = &noOpRetryMetric{}
	noopCbMetric    = &noOpCbMetric{}
)
