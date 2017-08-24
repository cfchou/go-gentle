package gentle

import "time"

// Metric is an interface for collecting metrics by
// RateLimitStream/RateLimitHandler/BulkheadStream/BulkheadHandler
type Metric interface {
	// Successful Stream.Get()/Handler.Handle() with timespan
	ObserveOk(timespan time.Duration)
	// Failed Stream.Get()/Handler.Handle() with timespan
	ObserveErr(timespan time.Duration)
}

// RetryMetric is an interface for collecting metrics by RetryStream/RetryHandler
type RetryMetric interface {
	// Successful Stream.Get()/Handler.Handle() with timespan and the number of retries
	ObserveOk(timespan time.Duration, retry int)
	// Failed Stream.Get()/Handler.Handle()  with timespan and the number of retries
	ObserveErr(timespan time.Duration, retry int)
}

// CbMetric is an interface for collecting metrics by CircuitStream/CircuitHandler
type CbMetric interface {
	// Successful Stream.Get()/Handler.Handle() with timespan
	ObserveOk(timespan time.Duration)
	// Failed Stream.Get()/Handler.Handle() with timespan and error.
	ObserveErr(timespan time.Duration, err error)
}

type noOpMetric struct{}

func (m *noOpMetric) ObserveOk(timespan time.Duration)  {}
func (m *noOpMetric) ObserveErr(timespan time.Duration) {}

type noOpRetryMetric struct{}

func (m *noOpRetryMetric) ObserveOk(timespan time.Duration, retry int)  {}
func (m *noOpRetryMetric) ObserveErr(timespan time.Duration, retry int) {}

type noOpCbMetric struct{}

func (m *noOpCbMetric) ObserveOk(timespan time.Duration)             {}
func (m *noOpCbMetric) ObserveErr(timespan time.Duration, err error) {}

var (
	noopMetric      = &noOpMetric{}
	noopRetryMetric = &noOpRetryMetric{}
	noopCbMetric    = &noOpCbMetric{}
)
