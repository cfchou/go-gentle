package metric

import (
	"errors"
	prom "github.com/prometheus/client_golang/prometheus"
	"gopkg.in/cfchou/go-gentle.v3/gentle"
	"time"
)

var (
	ErrInvalidName = errors.New("Invalid Name")
)

type PromMetric struct {
	name      string
	histVec   prom.HistogramVec
	okLabels  prom.Labels
	errLabels prom.Labels
}

func NewPromMetric(
	histOpts prom.HistogramOpts,
	okLabels, errLabels prom.Labels) *PromMetric {

	// union label names
	var labelSet map[string]struct{}
	for name, _ := range okLabels {
		labelSet[name] = struct{}{}
	}
	for name, _ := range errLabels {
		labelSet[name] = struct{}{}
	}
	var labelNames []string
	for name, _ := range labelSet {
		labelNames = append(labelNames, name)
	}

	return &PromMetric{
		histVec:   *prom.NewHistogramVec(histOpts, labelNames),
		okLabels:  okLabels,
		errLabels: errLabels,
	}
}

func (m *PromMetric) ObserveOk(timespan time.Duration) {
	m.histVec.With(m.okLabels).Observe(timespan.Seconds())
}

func (m *PromMetric) ObserveErr(timespan time.Duration) {
	m.histVec.With(m.errLabels).Observe(timespan.Seconds())
}

type PromRetryMetric struct {
	name       string
	histVec    prom.HistogramVec
	counterVec prom.CounterVec
	okLabels   prom.Labels
	errLabels  prom.Labels
}

func NewPromRetryMetric(
	histOpts prom.HistogramOpts,
	counterOpts prom.CounterOpts,
	okLabels, errLabels prom.Labels) *PromRetryMetric {

	// union label names
	var labelSet map[string]struct{}
	for name, _ := range okLabels {
		labelSet[name] = struct{}{}
	}
	for name, _ := range errLabels {
		labelSet[name] = struct{}{}
	}
	var labelNames []string
	for name, _ := range labelSet {
		labelNames = append(labelNames, name)
	}

	return &PromRetryMetric{
		histVec:    *prom.NewHistogramVec(histOpts, labelNames),
		counterVec: *prom.NewCounterVec(counterOpts, labelNames),
		okLabels:   okLabels,
		errLabels:  errLabels,
	}
}

func (m *PromRetryMetric) ObserveOk(timespan time.Duration, retry int) {
	m.histVec.With(m.okLabels).Observe(timespan.Seconds())
	m.counterVec.With(m.okLabels).Add(float64(retry))
}

func (m *PromRetryMetric) ObserveErr(timespan time.Duration, retry int) {
	m.histVec.With(m.errLabels).Observe(timespan.Seconds())
	m.counterVec.With(m.errLabels).Add(float64(retry))
}

type PromCbMetric struct {
	name       string
	histVec    prom.HistogramVec
	counterVec prom.CounterVec
	okLabels   prom.Labels
	errLabels  prom.Labels
}

func NewPromCbMetric(
	histOpts prom.HistogramOpts,
	counterOpts prom.CounterOpts,
	okLabels, errLabels prom.Labels) *PromCbMetric {

	// union label names
	var labelSet map[string]struct{}
	for name, _ := range okLabels {
		labelSet[name] = struct{}{}
	}
	for name, _ := range errLabels {
		// label "cbErr" will be silently overwritten
		labelSet[name] = struct{}{}
	}
	var labelNames []string
	for name, _ := range labelSet {
		labelNames = append(labelNames, name)
	}

	return &PromCbMetric{
		histVec:    *prom.NewHistogramVec(histOpts, labelNames),
		counterVec: *prom.NewCounterVec(counterOpts, labelNames),
		okLabels:   okLabels,
		errLabels:  errLabels,
	}
}

func (m *PromCbMetric) ObserveOk(timespan time.Duration) {
	m.histVec.With(m.okLabels).Observe(timespan.Seconds())
}

func (m *PromCbMetric) ObserveErr(timespan time.Duration, err error) {
	m.histVec.With(m.errLabels).Observe(timespan.Seconds())

	// If "cbErr" existed in m.errLabels, it'll be silently overwritten.
	errLabels := m.errLabels
	switch err {
	case gentle.ErrCbTimeout:
		errLabels["cbErr"] = "timeout"
		m.counterVec.With(errLabels).Add(1)
	case gentle.ErrMaxConcurrency:
		errLabels["cbErr"] = "maxConcurrency"
		m.counterVec.With(errLabels).Add(1)
	case gentle.ErrCbOpen:
		errLabels["cbErr"] = "open"
		m.counterVec.With(errLabels).Add(1)
	default:
		errLabels["cbErr"] = "other"
		m.counterVec.With(errLabels).Add(1)
	}
}
