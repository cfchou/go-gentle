package metric

import (
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/cfchou/go-gentle/gentle"
	"time"
)

// https://github.com/etsy/statsd/issues/22
// 1. stats.foo normalises by the flush interval so it's a counts per second
// 2. stats_counts.foo is the total count per flushInterval (default 10s).
// 3. stats.timers.foo.count is the total count per measurement interval.
// Counters(1 and 2) are constantly fed into Graphite.
// timers.Count on the other hand isn't. So Graphite will have null values which
// won't be used Graphite's aggregation(avg).
//
// To accommodate different scenarios, we may have timers and counters updated
// for one single metric.

// StatsdMetric implements gentle.Metric. It has a timer and a counter which are
// all subdivided into ok and err cases. Namely:
// stats.foo.ok
// stats.foo.err
// stats.timers.foo.ok
// stats.timers.foo.err
type StatsdMetric struct {
	Rate    float32
	SubOk   string
	SubErr  string
	statter statsd.SubStatter
}

func NewStatsdMetric(subPath string, client *statsd.Client) *StatsdMetric {
	return &StatsdMetric{
		Rate:    1,
		SubOk:   "ok",
		SubErr:  "err",
		statter: client.NewSubStatter(subPath),
	}
}

func (m *StatsdMetric) ObserveOk(timespan time.Duration) {
	m.statter.TimingDuration(m.SubOk, timespan, m.Rate)
	m.statter.Inc(m.SubOk, 1, m.Rate)
}

func (m *StatsdMetric) ObserveErr(timespan time.Duration) {
	m.statter.TimingDuration(m.SubErr, timespan, m.Rate)
	m.statter.Inc(m.SubErr, 1, m.Rate)
}

// StatsdRetryMetric implements gentle.RetryMetric. In addition to whatever
// StatsdMetric has, it adds a retry counter which is subdivided into ok and err
// cases. Namely:
// stats.foo.ok
// stats.foo.err
// stats.timers.foo.ok
// stats.timers.foo.err
//
// stats.foo.retry.ok
// stats.foo.retry.err
type StatsdRetryMetric struct {
	*StatsdMetric
	SubOk        string
	SubErr       string
	Rate         float32
	retryStatter statsd.SubStatter
}

func NewStatsdRetryMetric(subPath string, retrySubPath string, client *statsd.Client) *StatsdRetryMetric {
	m := NewStatsdMetric(subPath, client)
	var retryStatter statsd.SubStatter
	if retrySubPath == "" {
		retryStatter = m.statter.NewSubStatter("retry")
	} else {
		retryStatter = client.NewSubStatter(retrySubPath)
	}
	return &StatsdRetryMetric{
		StatsdMetric: m,
		SubOk:        m.SubOk,
		SubErr:       m.SubErr,
		Rate:         m.Rate,
		retryStatter: retryStatter,
	}
}

func (m *StatsdRetryMetric) ObserveOk(timespan time.Duration, retry int) {
	m.StatsdMetric.ObserveOk(timespan)
	m.retryStatter.Inc(m.SubOk, int64(retry), m.Rate)
}

func (m *StatsdRetryMetric) ObserveErr(timespan time.Duration, retry int) {
	m.StatsdMetric.ObserveErr(timespan)
	m.retryStatter.Inc(m.SubErr, int64(retry), m.Rate)
}

// StatsdCbMetric implements gentle.CbMetric. In addition to whatever
// StatsdMetric has, it adds counters of circuit errors. Namely:
// stats.foo.ok
// stats.foo.err
// stats.timers.foo.ok
// stats.timers.foo.err
//
// stats.foo.cbErr.timeout
// stats.foo.cbErr.maxConcurrency
// stats.foo.cbErr.open
type StatsdCbMetric struct {
	*StatsdMetric
	SubTimeout        string
	SubMaxConcurrency string
	SubOpen           string
	Rate              float32
	cbErrStatter      statsd.SubStatter
}

func NewStatsdCbMetric(subPath string, cbErrSubPath string, client *statsd.Client) *StatsdCbMetric {
	m := NewStatsdMetric(subPath, client)
	var cbErrStatter statsd.SubStatter
	if cbErrSubPath == "" {
		cbErrStatter = m.statter.NewSubStatter("cbErr")
	} else {
		cbErrStatter = client.NewSubStatter(cbErrSubPath)
	}
	return &StatsdCbMetric{
		StatsdMetric:      m,
		SubTimeout:        "timeout",
		SubMaxConcurrency: "maxConcurrency",
		SubOpen:           "open",
		Rate:              m.Rate,
		cbErrStatter:      cbErrStatter,
	}
}

func (m *StatsdCbMetric) ObserveOk(timespan time.Duration) {
	m.StatsdMetric.ObserveOk(timespan)
}

func (m *StatsdCbMetric) ObserveErr(timespan time.Duration, err error) {
	m.StatsdMetric.ObserveErr(timespan)
	switch err {
	case gentle.ErrCbTimeout:
		m.cbErrStatter.Inc(m.SubTimeout, 1, m.Rate)
	case gentle.ErrMaxConcurrency:
		m.cbErrStatter.Inc(m.SubMaxConcurrency, 1, m.Rate)
	case gentle.ErrCbOpen:
		m.cbErrStatter.Inc(m.SubOpen, 1, m.Rate)
	default:
		// others are ignored
	}
}
