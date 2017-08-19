package gentle

import (
	"context"
	"errors"
	"fmt"
	"github.com/rs/xid"
	"math/rand"
	"sync"
	"time"
)

func ExampleNewRateLimitedStream() {
	var fakeStream SimpleStream = func(_ context.Context) (Message, error) {
		return &fakeMsg{xid.New().String()}, nil
	}

	count := 5
	interval := 100 * time.Millisecond
	minimum := time.Duration(count-1) * interval

	stream := NewRateLimitedStream(
		NewRateLimitedStreamOpts("", "test",
			NewTokenBucketRateLimit(interval, 1)),
		fakeStream)

	begin := time.Now()
	wg := &sync.WaitGroup{}
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			stream.Get(context.Background())
		}()
	}
	wg.Wait()
	fmt.Printf("Spend more than %s? %t\n", minimum,
		time.Now().After(begin.Add(minimum)))
	// Output: Spend more than 400ms? true
}

func ExampleNewRetryStream_contantBackOff() {
	fakeErr := errors.New("fake err")
	var fakeStream SimpleStream = func(_ context.Context) (Message, error) {
		// return fakeErr to trigger back-off
		return nil, fakeErr
	}
	// fakeStream keeps triggering back-offs.
	// No more back-off when total execution + back-offs elapsed more than 1s.
	backOffOpts := NewConstantBackOffFactoryOpts(100*time.Millisecond, time.Second)
	backOffFactory := NewConstantBackOffFactory(backOffOpts)
	opts := NewRetryStreamOpts("", "test", backOffFactory)
	stream := NewRetryStream(opts, fakeStream)
	stream.Get(context.Background())
	// Output:
}

func ExampleNewRetryStream_exponentialBackOff() {
	fakeErr := errors.New("fake err")
	var fakeStream SimpleStream = func(_ context.Context) (Message, error) {
		// return fakeErr to trigger back-off
		return nil, fakeErr
	}
	// fakeStream keeps triggering back-offs.
	// No more back-off when total execution + back-offs elapsed more than 2s.
	backOffOpts := NewExponentialBackOffFactoryOpts(100*time.Millisecond, 2,
		time.Second, 2*time.Second)
	backOffFactory := NewExponentialBackOffFactory(backOffOpts)
	opts := NewRetryStreamOpts("", "test", backOffFactory)
	stream := NewRetryStream(opts, fakeStream)
	stream.Get(context.Background())
	// Output:
}

func ExampleNewBulkheadStream() {
	var fakeStream SimpleStream = func(_ context.Context) (Message, error) {
		return &fakeMsg{xid.New().String()}, nil
	}

	count := 10
	// no more than 2 concurrent access to fakeStream
	stream := NewBulkheadStream(
		NewBulkheadStreamOpts("", "test", 2),
		fakeStream)

	wg := &sync.WaitGroup{}
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			msg, err := stream.Get(context.Background())
			if err != nil {
				if err == ErrMaxConcurrency {
					fmt.Println("Reached MaxConcurrency")
				} else {
					fmt.Println("Other err:", err)
				}
			} else {
				fmt.Println("msg:", msg.ID())
			}
		}()
	}
	wg.Wait()
}

func ExampleNewCircuitBreakerStream() {
	defer CircuitBreakerReset()
	rand.Seed(time.Now().UnixNano())
	fakeErr := errors.New("fake err")
	var fakeStream SimpleStream = func(_ context.Context) (Message, error) {
		if rand.Intn(10)%10 == 0 {
			// 1/10 chances to return fakeErr
			return nil, fakeErr
		}
		if rand.Intn(10)%10 == 1 {
			// 1/10 chances to sleep until circuit's timeout
			time.Sleep(DefaultCbTimeout + 10*time.Millisecond)
			return &fakeMsg{xid.New().String()}, nil
		}
		return &fakeMsg{xid.New().String()}, nil
	}

	stream := NewCircuitBreakerStream(
		NewCircuitBreakerStreamOpts("", "test", xid.New().String()),
		fakeStream)

	count := 100
	wg := &sync.WaitGroup{}
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			msg, err := stream.Get(context.Background())
			if err != nil {
				switch err {
				case ErrCbMaxConcurrency:
					fmt.Println("Reached Circuit's MaxConcurrency")
				case ErrCbTimeout:
					fmt.Println("Reached Circuit's Timeout")
				case ErrCbOpen:
					fmt.Println("Reached Circuit's threshold so it opens")
				default:
					fmt.Println("Other err:", err)
				}
				return
			}
			fmt.Println("msg:", msg.ID())
		}()
	}
	wg.Wait()
}

func ExampleNewCircuitBreakerStream_customCircuit() {
	defer CircuitBreakerReset()
	rand.Seed(time.Now().UnixNano())
	fakeErr := errors.New("fake err")
	var fakeStream SimpleStream = func(_ context.Context) (Message, error) {
		if rand.Intn(10)%10 == 0 {
			// 1/10 chances to return fakeErr
			return nil, fakeErr
		}
		if rand.Intn(10)%10 == 1 {
			// 1/10 chances to sleep until circuit's timeout
			time.Sleep(DefaultCbTimeout + 10*time.Millisecond)
			return &fakeMsg{xid.New().String()}, nil
		}
		return &fakeMsg{xid.New().String()}, nil
	}

	circuit := xid.New().String()
	conf := NewDefaultCircuitBreakerConf()
	conf.MaxConcurrent = 20
	conf.RegisterFor(circuit)

	stream := NewCircuitBreakerStream(
		NewCircuitBreakerStreamOpts("", "test", circuit),
		fakeStream)

	count := 100
	wg := &sync.WaitGroup{}
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			msg, err := stream.Get(context.Background())
			if err != nil {
				switch err {
				case ErrCbMaxConcurrency:
					fmt.Println("Reached Circuit's MaxConcurrency")
				case ErrCbTimeout:
					fmt.Println("Reached Circuit's Timeout")
				case ErrCbOpen:
					fmt.Println("Reached Circuit's threshold so it opens")
				default:
					fmt.Println("Other err:", err)
				}
				return
			}
			fmt.Println("msg:", msg.ID())
		}()
	}
	wg.Wait()
}
