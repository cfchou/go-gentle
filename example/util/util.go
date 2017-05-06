package util

import (
	"time"
	"os"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/cfchou/go-gentle/gentle"
)

// Based on times_per_sec to calculate the interval in millis between every
// request.
func FreqToIntervalMillis(times_per_sec int) time.Duration {
	n := time.Millisecond / time.Duration(times_per_sec)
	if n < 1 {
		Log.Error("Interval should be no less than 1 millisec",
			"interval", n)
		os.Exit(-1)
	}
	Log.Info("Interval: %s\n", "interval", n)
	return n
}

// Based on times_per_sec to calculate the interval in micros between every
// request.
func FreqToIntervalMicros(times_per_sec int) time.Duration {
	n := time.Microsecond / time.Duration(times_per_sec)
	if n < 1 {
		Log.Error("Interval should be no less than 1 microsec",
			"interval", n)
		os.Exit(-1)
	}
	Log.Info("Interval: %s\n", "interval", n)
	return n
}


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

// Histogram:
// namespace_sList_get_seconds{name, result}
func RegisterGmailListStreamMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace,
				   MIXIN_STREAM_GMAIL_LIST,
				   "", gentle.MX_STREAM_GET}
	if _, err := gentle.GetObservation(key); err == nil {
		// registered
		return
	}
	histVec := prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: namespace,
			Subsystem: MIXIN_STREAM_GMAIL_LIST,
			Name:      "get_seconds",
			Help:      "Duration of GmailListStream.Get() in seconds",
			Buckets:   prom.DefBuckets,
		},
		[]string{"name", "api", "result"})
	prom.MustRegister(histVec)
	gentle.RegisterObservation(key, &promHist{
		name:    "",
		histVec: histVec,
	})
}

// Histogram:
// namespace_hDownload_handle_seconds{name, result}
// namespace_hDownload_msg_bytes{name, result}
func RegisterGmailMessageHandlerMetrics(namespace, name string) {
	key := &gentle.RegistryKey{namespace,
				   MIXIN_HANDLER_GMAIL_DOWNLOAD,
				   "", gentle.MX_HANDLER_HANDLE}
	if _, err := gentle.GetObservation(key); err != nil {
		histVec := prom.NewHistogramVec(
			prom.HistogramOpts{
				Namespace: namespace,
				Subsystem: MIXIN_HANDLER_GMAIL_DOWNLOAD,
				Name:      "handle_seconds",
				Help:      "Duration of GmailMessageHandler.Handle() in seconds",
				Buckets:   prom.DefBuckets,
			},
			[]string{"name", "api", "result"})
		prom.MustRegister(histVec)
		gentle.RegisterObservation(key, &promHist{
			name:    "",
			histVec: histVec,
		})
	}
	key = &gentle.RegistryKey{namespace,
				  MIXIN_HANDLER_GMAIL_DOWNLOAD,
				  name, MX_HANDLER_GMAIL_SIZE}
	if _, err := gentle.GetObservation(key); err != nil {
		histVec := prom.NewHistogramVec(
			prom.HistogramOpts{
				Namespace: namespace,
				Subsystem: MIXIN_HANDLER_GMAIL_DOWNLOAD,
				Name:      "msg_bytes",
				Help:      "Size of a message in bytes that GmailMessageHandler.Handle() downloads",
				// 5k, 10k, 20k, 40k, ...., 1M, 2M, +Inf
				Buckets:   []float64{
					1024*5, 1024*10, 1024*20, 1024*40,
					1024*80, 1024*160, 1024*320, 1024*640,
					1024*1280, 1024*2048,
				},
			},
			[]string{"name"})
		prom.MustRegister(histVec)
		gentle.RegisterObservation(key, &promHist{
			name:    name,
			histVec: histVec,
		})
	}
}

