package metric

import (
	"errors"
	prom "github.com/prometheus/client_golang/prometheus"
	"gopkg.in/cfchou/go-gentle.v3/gentle"
	"time"
)

var (
	ErrInvalidLabel = errors.New("Invalid label")
)

type ResultLabel struct {
	Name     string
	ValueOk  string
	ValueErr string
}

type PromMetric struct {
	name     string
	histVec  *prom.HistogramVec
	okLabel  prom.Labels
	errLabel prom.Labels
}

func NewPromMetric(
	histOpts prom.HistogramOpts,
	resultLabel ResultLabel) (*PromMetric, error) {

	for name := range histOpts.ConstLabels {
		if name == resultLabel.Name {
			// a label can only be either constant or dynamic(ok/err)
			return nil, ErrInvalidLabel
		}
	}

	histVec := prom.NewHistogramVec(histOpts, []string{resultLabel.Name})
	prom.MustRegister(histVec)
	return &PromMetric{
		histVec:  histVec,
		okLabel:  prom.Labels{resultLabel.Name: resultLabel.ValueOk},
		errLabel: prom.Labels{resultLabel.Name: resultLabel.ValueErr},
	}, nil
}

func (m *PromMetric) ObserveOk(timespan time.Duration) {
	m.histVec.With(m.okLabel).Observe(timespan.Seconds())
}

func (m *PromMetric) ObserveErr(timespan time.Duration) {
	m.histVec.With(m.errLabel).Observe(timespan.Seconds())
}

type PromRetryMetric struct {
	name          string
	histVec       *prom.HistogramVec
	counterVec    *prom.CounterVec
	okLabel       prom.Labels
	errLabel      prom.Labels
	retryOkLabel  prom.Labels
	retryErrLabel prom.Labels
}

func NewPromRetryMetric(
	histOpts prom.HistogramOpts,
	counterOpts prom.CounterOpts,
	resultLabel, retryLabel ResultLabel) (*PromRetryMetric, error) {

	for name := range histOpts.ConstLabels {
		if name == resultLabel.Name || name == retryLabel.Name {
			// a label can only be either constant or dynamic(ok/err)
			return nil, ErrInvalidLabel
		}
	}

	histVec := prom.NewHistogramVec(histOpts, []string{resultLabel.Name})
	prom.MustRegister(histVec)
	counterVec := prom.NewCounterVec(counterOpts, []string{retryLabel.Name})
	prom.MustRegister(counterVec)

	return &PromRetryMetric{
		histVec:       histVec,
		counterVec:    counterVec,
		okLabel:       prom.Labels{resultLabel.Name: resultLabel.ValueOk},
		errLabel:      prom.Labels{resultLabel.Name: resultLabel.ValueErr},
		retryOkLabel:  prom.Labels{retryLabel.Name: retryLabel.ValueOk},
		retryErrLabel: prom.Labels{retryLabel.Name: retryLabel.ValueErr},
	}, nil
}

func (m *PromRetryMetric) ObserveOk(timespan time.Duration, retry int) {
	m.histVec.With(m.okLabel).Observe(timespan.Seconds())
	m.counterVec.With(m.retryOkLabel).Add(float64(retry))
}

func (m *PromRetryMetric) ObserveErr(timespan time.Duration, retry int) {
	m.histVec.With(m.errLabel).Observe(timespan.Seconds())
	m.counterVec.With(m.retryErrLabel).Add(float64(retry))
}

type PromCbMetric struct {
	name        string
	histVec     *prom.HistogramVec
	counterVec  *prom.CounterVec
	okLabel     prom.Labels
	errLabel    prom.Labels
	cbErrLabels map[string]prom.Labels
}

func NewPromCbMetric(
	histOpts prom.HistogramOpts,
	counterOpts prom.CounterOpts,
	resultLabel ResultLabel, cbErrLabelName string) (*PromCbMetric, error) {

	for name := range histOpts.ConstLabels {
		if name == resultLabel.Name || name == cbErrLabelName {
			// a label can only be either constant or dynamic(ok/err)
			return nil, ErrInvalidLabel
		}
	}

	histVec := prom.NewHistogramVec(histOpts, []string{resultLabel.Name})
	prom.MustRegister(histVec)
	counterVec := prom.NewCounterVec(counterOpts, []string{cbErrLabelName})
	prom.MustRegister(counterVec)

	return &PromCbMetric{
		histVec:    histVec,
		counterVec: counterVec,
		okLabel:    prom.Labels{resultLabel.Name: resultLabel.ValueOk},
		errLabel:   prom.Labels{resultLabel.Name: resultLabel.ValueErr},
		cbErrLabels: map[string]prom.Labels{
			"timeout": prom.Labels{cbErrLabelName: "timeout"},
			"max":     prom.Labels{cbErrLabelName: "max"},
			"open":    prom.Labels{cbErrLabelName: "open"},
			"other":   prom.Labels{cbErrLabelName: "other"},
		},
	}, nil
}

func (m *PromCbMetric) ObserveOk(timespan time.Duration) {
	m.histVec.With(m.okLabel).Observe(timespan.Seconds())
}

func (m *PromCbMetric) ObserveErr(timespan time.Duration, err error) {
	m.histVec.With(m.errLabel).Observe(timespan.Seconds())

	switch err {
	case gentle.ErrCbTimeout:
		m.counterVec.With(m.cbErrLabels["timeout"]).Add(1)
	case gentle.ErrMaxConcurrency:
		m.counterVec.With(m.cbErrLabels["max"]).Add(1)
	case gentle.ErrCbOpen:
		m.counterVec.With(m.cbErrLabels["open"]).Add(1)
	default:
		m.counterVec.With(m.cbErrLabels["other"]).Add(1)
	}
}
