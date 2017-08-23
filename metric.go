package go_gentle

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

// CbMetric is an interface for collecting metrics by CircuitStream/CircuitHandler
type CbMetric interface {
	// Successful Stream.Get()/Handler.Handle() with timespan in seconds
	ObserveOk(timespan float64)
	// Failed Stream.Get()/Handler.Handle() with timespan in seconds and error.
	ObserveErr(timespan float64, err error)
}
