package extra

import (
	//"gopkg.in/cfchou/go-gentle.v1/gentle"
	"github.com/prometheus/client_golang/prometheus"
	""
)


type PrometheusMetric struct {
	basename string

}

func NewPrometheusMetric(basename string) *PrometheusMetric {
	return &PrometheusMetric{
		basename:basename,
	}
}

func (m *PrometheusMetric) BaseName() string {
	return m.basename
}

func (m *PrometheusMetric) CounterAdd(name string, delta float64) {

}


/*
NewXXXStream(metric Metric) {
	gauge := metric.RegisterGauge(XXX.name.id})
	gauge2 := metric.RegisterGauge(XXX.name.id2})
	counter := metric.RegisterCounter({XXX.name.id})
	...
}
 */





