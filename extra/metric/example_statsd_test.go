package metric

import (
	"context"
	"errors"
	"github.com/cactus/go-statsd-client/statsd"
	log2 "github.com/cfchou/go-gentle/extra/log"
	"gopkg.in/cfchou/go-gentle.v3/gentle"
	"gopkg.in/inconshreveable/log15.v2"
	"log"
	"net"
	"sync"
	"time"
)

func ExampleNewStatsdMetric() {
	subPath := "example.test1"
	client, _ := statsd.NewClient("127.0.0.1:8125", "extra")
	opts := gentle.NewRateLimitedStreamOpts("", "test1",
		gentle.NewTokenBucketRateLimit(10*time.Millisecond, 1))
	opts.Metric = NewStatsdMetric(subPath, client)
	opts.Log = log2.NewLog15Adapter(log15.New())

	var upstream gentle.SimpleStream = func(ctx context.Context) (gentle.Message, error) {
		return gentle.SimpleMessage(""), nil
	}
	stream := gentle.NewRateLimitedStream(opts, upstream)

	// listen like "nc 8125 -l -u"
	conn, err := net.ListenPacket("udp", ":8125")
	if err != nil {
		log.Fatalf("ListenPacket err: %s\n", err)
	}
	defer conn.Close()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		bs := make([]byte, 256)
		// read 5 times should be enough
		for i := 0; i < 5; i++ {
			conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			n, _, err := conn.ReadFrom(bs)
			if err != nil {
				log.Printf("ReadFrom err: %s\n", err)
				continue
			}
			// timer:
			// 	extra.rate.test1.ok:0.020875|ms
			// counter:
			// 	extra.rate.test1.ok:1|c
			log.Println(string(bs[:n]))
		}
	}()

	stream.Get(context.Background())
	wg.Wait()
	// Output:
}

func ExampleNewStatsdRetryMetric() {
	subPath := "example.test2.result"
	retrySubPath := "example.test2.retry"
	client, _ := statsd.NewClient("127.0.0.1:8125", "extra")
	factory := gentle.NewConstantBackOffFactory(
		gentle.NewConstantBackOffFactoryOpts(10*time.Millisecond, time.Second))
	opts := gentle.NewRetryStreamOpts("", "test2", factory)
	opts.RetryMetric = NewStatsdRetryMetric(subPath, retrySubPath, client)
	opts.Log = log2.NewLog15Adapter(log15.New())

	count := 5
	var upstream gentle.SimpleStream = func(ctx context.Context) (gentle.Message, error) {
		if count == 0 {
			return gentle.SimpleMessage(""), nil
		}
		count--
		return nil, errors.New("fake err")
	}
	stream := gentle.NewRetryStream(opts, upstream)

	// listen like "nc 8125 -l -u"
	conn, err := net.ListenPacket("udp", ":8125")
	if err != nil {
		log.Fatalf("ListenPacket err: %s\n", err)
	}
	defer conn.Close()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		bs := make([]byte, 256)
		// read 5 times should be enough
		for i := 0; i < 5; i++ {
			conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			n, _, err := conn.ReadFrom(bs)
			if err != nil {
				log.Printf("ReadFrom err: %s\n", err)
				continue
			}
			// timer:
			// 	extra.example.test2.result.ok:56.73204|ms
			// counter:
			// 	extra.example.test2.result.ok:1|c
			// retry-counter:
			// 	extra.example.test2.retry.ok:5|c
			log.Println(string(bs[:n]))
		}
	}()

	stream.Get(context.Background())
	wg.Wait()
	// Output:
}

func ExampleNewStatsdCbMetric() {
	gentle.CircuitReset()
	subPath := "example.test3.result"
	cbSubPath := "example.test3.cb"
	client, _ := statsd.NewClient("127.0.0.1:8125", "extra")
	opts := gentle.NewCircuitStreamOpts("", "test3", "test_circuit")
	opts.CbMetric = NewStatsdCbMetric(subPath, cbSubPath, client)
	opts.Log = log2.NewLog15Adapter(log15.New())

	var upstream gentle.SimpleStream = func(ctx context.Context) (gentle.Message, error) {
		return nil, gentle.ErrCbOpen
	}
	stream := gentle.NewCircuitStream(opts, upstream)

	// listen like "nc 8125 -l -u"
	conn, err := net.ListenPacket("udp", ":8125")
	if err != nil {
		log.Fatalf("ListenPacket err: %s\n", err)
	}
	defer conn.Close()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		bs := make([]byte, 256)
		// read 5 times should be enough
		for i := 0; i < 5; i++ {
			conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			n, _, err := conn.ReadFrom(bs)
			if err != nil {
				log.Printf("ReadFrom err: %s\n", err)
				continue
			}
			// timer:
			// 	extra.example.test3.result.err:1.358839|ms
			// counter:
			// 	extra.example.test3.result.err:1|c
			// cb-error-counter:
			// 	extra.example.test3.cb.open:1|c
			log.Println(string(bs[:n]))
		}
	}()

	stream.Get(context.Background())
	wg.Wait()
	// Output:
}
