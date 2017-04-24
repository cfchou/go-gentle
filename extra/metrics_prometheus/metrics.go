package metrics_prometheus

import prom "github.com/prometheus/client_golang/prometheus"

type promHist struct {
	name    string
	histVec *prom.HistogramVec
}

func (p *promHist) Observe(value float64, labels map[string]string) {
	m := map[string]string{"name": p.name}
	for k, v := range labels {
		m[k] = v
	}
	h := p.histVec.With(m)
	h.Observe(value)
}

type promCounter struct {
	name       string
	counterVec *prom.CounterVec
}

func (p *promCounter) Observe(value float64, labels map[string]string) {
	m := map[string]string{"name": p.name}
	for k, v := range labels {
		m[k] = v
	}
	c := p.counterVec.With(m)
	c.Add(value)
}
