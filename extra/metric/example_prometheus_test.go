package metric

import (
	"context"
	"errors"
	prom "github.com/prometheus/client_golang/prometheus"
	promhttp "github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/cfchou/go-gentle.v3/gentle"
	"io/ioutil"
	"log"
	"net/http"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	srv := startHttp()
	defer srv.Shutdown(nil)
	m.Run()
}

func startHttp() *http.Server {
	srv := &http.Server{
		Addr: ":8080",
	}

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("ListenAndServe() err: %s\n", err)
		}
	}()
	return srv
}

func scrap() ([]byte, error) {
	resp, err := http.Get("http://localhost:8080/metrics")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	bs, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return bs, nil
}

func ExampleNewPromMetric() {
	opts := gentle.NewRateLimitedStreamOpts("", "test1",
		gentle.NewTokenBucketRateLimit(time.Second, 1))
	opts.Metric, _ = NewPromMetric(prom.HistogramOpts{
		Help:      "duration of RateLimitedStream.Get()",
		Namespace: "test",
		Subsystem: "rate",
		Name:      "example_seconds",
		Buckets:   []float64{0.1, 0.5, 1, 2.5},
		ConstLabels: prom.Labels{
			"host": "localhost",
		},
	}, ResultLabel{
		Name:     "result",
		ValueOk:  "ok",
		ValueErr: "err",
	})

	okRun := 3
	totalRun := 6
	var upstream gentle.SimpleStream = func(ctx context.Context) (gentle.Message, error) {
		if okRun > 0 {
			okRun--
			return gentle.SimpleMessage(""), nil
		}
		return nil, errors.New("fake err")
	}

	stream := gentle.NewRateLimitedStream(opts, upstream)
	for i := 0; i < totalRun; i++ {
		stream.Get(context.Background())
	}

	bs, err := scrap()
	if err != nil {
		log.Fatalf("scrap() err: %s\n", err)
	}
	// # HELP test_rate_example_seconds duration of RateLimitedStream.Get()
	// # TYPE test_rate_example_seconds histogram
	// test_rate_example_seconds_bucket{host="localhost",result="err",le="0.1"} 0
	// test_rate_example_seconds_bucket{host="localhost",result="err",le="0.5"} 0
	// test_rate_example_seconds_bucket{host="localhost",result="err",le="1"} 1
	// test_rate_example_seconds_bucket{host="localhost",result="err",le="2.5"} 3
	// test_rate_example_seconds_bucket{host="localhost",result="err",le="+Inf"} 3
	// test_rate_example_seconds_sum{host="localhost",result="err"} 2.998811008
	// test_rate_example_seconds_count{host="localhost",result="err"} 3
	// test_rate_example_seconds_bucket{host="localhost",result="ok",le="0.1"} 1
	// test_rate_example_seconds_bucket{host="localhost",result="ok",le="0.5"} 1
	// test_rate_example_seconds_bucket{host="localhost",result="ok",le="1"} 2
	// test_rate_example_seconds_bucket{host="localhost",result="ok",le="2.5"} 3
	// test_rate_example_seconds_bucket{host="localhost",result="ok",le="+Inf"} 3
	// test_rate_example_seconds_sum{host="localhost",result="ok"} 2.001090599
	// test_rate_example_seconds_count{host="localhost",result="ok"} 3
	log.Print(string(bs))
	// Output:
}

func ExampleNewPromRetryMetric() {
	factory := gentle.NewConstBackOffFactory(
		gentle.NewConstBackOffFactoryOpts(100*time.Millisecond, time.Second))
	opts := gentle.NewRetryStreamOpts("", "test2", factory)
	opts.RetryMetric, _ = NewPromRetryMetric(prom.HistogramOpts{
		Help:      "duration of RetryStream.Get()",
		Namespace: "test",
		Subsystem: "retry",
		Name:      "example_seconds",
		Buckets:   []float64{0.1, 0.5, 1, 2.5},
		ConstLabels: prom.Labels{
			"host": "localhost",
		},
	}, prom.CounterOpts{
		Help:      "total of retries",
		Namespace: "test",
		Subsystem: "retry",
		Name:      "example_total",
		ConstLabels: prom.Labels{
			"host": "localhost",
		},
	}, ResultLabel{
		Name:     "result",
		ValueOk:  "ok",
		ValueErr: "err",
	}, ResultLabel{
		Name:     "retry",
		ValueOk:  "ok",
		ValueErr: "err",
	})

	okRun := 3
	totalRun := 6
	retryCount := 5
	var upstream gentle.SimpleStream = func(ctx context.Context) (gentle.Message, error) {
		if okRun > 0 {
			// retry-run
			if retryCount == 0 {
				okRun--
				// reset for another okRun
				retryCount = 5
				return gentle.SimpleMessage(""), nil
			}
			retryCount--
			return nil, errors.New("fake err")
		}
		// errRun: always fall
		return nil, errors.New("fake err")
	}

	stream := gentle.NewRetryStream(opts, upstream)
	for i := 0; i < totalRun; i++ {
		stream.Get(context.Background())
	}

	bs, err := scrap()
	if err != nil {
		log.Fatalf("scrap() err: %s\n", err)
	}
	// # HELP test_retry_example_seconds duration of RetryStream.Get()
	// # TYPE test_retry_example_seconds histogram
	// test_retry_example_seconds_bucket{host="localhost",result="err",le="0.1"} 0
	// test_retry_example_seconds_bucket{host="localhost",result="err",le="0.5"} 0
	// test_retry_example_seconds_bucket{host="localhost",result="err",le="1"} 0
	// test_retry_example_seconds_bucket{host="localhost",result="err",le="2.5"} 3
	// test_retry_example_seconds_bucket{host="localhost",result="err",le="+Inf"} 3
	// test_retry_example_seconds_sum{host="localhost",result="err"} 3.07845743
	// test_retry_example_seconds_count{host="localhost",result="err"} 3
	// test_retry_example_seconds_bucket{host="localhost",result="ok",le="0.1"} 0
	// test_retry_example_seconds_bucket{host="localhost",result="ok",le="0.5"} 0
	// test_retry_example_seconds_bucket{host="localhost",result="ok",le="1"} 3
	// test_retry_example_seconds_bucket{host="localhost",result="ok",le="2.5"} 3
	// test_retry_example_seconds_bucket{host="localhost",result="ok",le="+Inf"} 3
	// test_retry_example_seconds_sum{host="localhost",result="ok"} 1.554815478
	// test_retry_example_seconds_count{host="localhost",result="ok"} 3
	// # HELP test_retry_example_total total of retries
	// # TYPE test_retry_example_total counter
	// test_retry_example_total{host="localhost",retry="err"} 30
	// test_retry_example_total{host="localhost",retry="ok"} 15
	log.Print(string(bs))
	// Output:
}

func ExampleNewPromCbMetric() {
	gentle.CircuitReset()
	opts := gentle.NewCircuitStreamOpts("", "test3", "test_circuit")
	opts.CbMetric, _ = NewPromCbMetric(prom.HistogramOpts{
		Help:      "duration of CircuitStream.Get()",
		Namespace: "test",
		Subsystem: "circuit",
		Name:      "example_seconds",
		Buckets:   []float64{0.1, 0.5, 1, 2.5},
		ConstLabels: prom.Labels{
			"host": "localhost",
		},
	}, prom.CounterOpts{
		Help:      "totals of different errors",
		Namespace: "test",
		Subsystem: "circuit",
		Name:      "example_total",
		ConstLabels: prom.Labels{
			"host": "localhost",
		},
	}, ResultLabel{
		Name:     "result",
		ValueOk:  "ok",
		ValueErr: "err",
	}, "cbErr")

	okRun := 3
	totalRun := 6
	var upstream gentle.SimpleStream = func(ctx context.Context) (gentle.Message, error) {
		if okRun > 0 {
			okRun--
			return gentle.SimpleMessage(""), nil
		}
		return nil, gentle.ErrCbOpen
	}

	stream := gentle.NewCircuitStream(opts, upstream)
	for i := 0; i < totalRun; i++ {
		stream.Get(context.Background())
	}

	bs, err := scrap()
	if err != nil {
		log.Fatalf("scrap() err: %s\n", err)
	}
	// # HELP test_circuit_example_seconds duration of CircuitStream.Get()
	// # TYPE test_circuit_example_seconds histogram
	// test_circuit_example_seconds_bucket{host="localhost",result="err",le="0.1"} 3
	// test_circuit_example_seconds_bucket{host="localhost",result="err",le="0.5"} 3
	// test_circuit_example_seconds_bucket{host="localhost",result="err",le="1"} 3
	// test_circuit_example_seconds_bucket{host="localhost",result="err",le="2.5"} 3
	// test_circuit_example_seconds_bucket{host="localhost",result="err",le="+Inf"} 3
	// test_circuit_example_seconds_sum{host="localhost",result="err"} 0.000357439
	// test_circuit_example_seconds_count{host="localhost",result="err"} 3
	// test_circuit_example_seconds_bucket{host="localhost",result="ok",le="0.1"} 3
	// test_circuit_example_seconds_bucket{host="localhost",result="ok",le="0.5"} 3
	// test_circuit_example_seconds_bucket{host="localhost",result="ok",le="1"} 3
	// test_circuit_example_seconds_bucket{host="localhost",result="ok",le="2.5"} 3
	// test_circuit_example_seconds_bucket{host="localhost",result="ok",le="+Inf"} 3
	// test_circuit_example_seconds_sum{host="localhost",result="ok"} 0.000684208
	// test_circuit_example_seconds_count{host="localhost",result="ok"} 3
	// # HELP test_circuit_example_total totals of different errors
	// # TYPE test_circuit_example_total counter
	// test_circuit_example_total{cbErr="open",host="localhost"} 3
	log.Print(string(bs))
	// Output:
}
