package gentle

import (
	"context"
	"errors"
	"fmt"
	"github.com/rs/xid"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

func ExampleSimpleStream() {
	msgID := 1
	var stream SimpleStream = func(_ context.Context) (Message, error) {
		defer func() { msgID++ }()
		return SimpleMessage(strconv.Itoa(msgID)), nil
	}

	for i := 0; i < 5; i++ {
		msg, _ := stream.Get(context.Background())
		fmt.Println("msg:", msg.ID())
	}
	// Output:
	// msg: 1
	// msg: 2
	// msg: 3
	// msg: 4
	// msg: 5
}

func ExampleNewRateLimitedStream() {
	var msgID int64
	var fakeStream SimpleStream = func(_ context.Context) (Message, error) {
		id := atomic.AddInt64(&msgID, 1)
		return SimpleMessage(strconv.FormatInt(id, 10)), nil
	}

	count := 5
	interval := 100 * time.Millisecond
	minimum := time.Duration(count-1) * interval

	// limit the rate to access fakeStream
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
	// fakeStream keeps triggering back-offs.
	var fakeStream SimpleStream = func(_ context.Context) (Message, error) {
		return nil, fakeErr
	}

	// No more back-off when total execution + back-offs elapsed more than 1s.
	backOffOpts := NewConstantBackOffFactoryOpts(100*time.Millisecond, time.Second)
	backOffFactory := NewConstantBackOffFactory(backOffOpts)
	opts := NewRetryStreamOpts("", "test", backOffFactory)
	// Retry with back-offs to access fakeStream
	stream := NewRetryStream(opts, fakeStream)
	_, err := stream.Get(context.Background())
	fmt.Println(err)
	// Output: fake err
}

func ExampleNewRetryStream_exponentialBackOff() {
	fakeErr := errors.New("fake err")
	// fakeStream keeps triggering back-offs.
	var fakeStream SimpleStream = func(_ context.Context) (Message, error) {
		return nil, fakeErr
	}

	// No more back-off when total execution + back-offs elapsed more than 2s.
	backOffOpts := NewExponentialBackOffFactoryOpts(100*time.Millisecond, 2,
		time.Second, 2*time.Second)
	backOffFactory := NewExponentialBackOffFactory(backOffOpts)
	opts := NewRetryStreamOpts("", "test", backOffFactory)
	// Retry with back-offs to access fakeStream
	stream := NewRetryStream(opts, fakeStream)
	_, err := stream.Get(context.Background())
	fmt.Println(err)
	// Output: fake err
}

func ExampleNewBulkheadStream() {
	var msgID int64
	var fakeStream SimpleStream = func(_ context.Context) (Message, error) {
		id := atomic.AddInt64(&msgID, 1)
		return SimpleMessage(strconv.FormatInt(id, 10)), nil
	}

	count := 10
	// limit concurrent access to fakeStream
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

func ExampleNewCircuitStream() {
	rand.Seed(time.Now().UnixNano())
	fakeErr := errors.New("fake err")
	var msgID int64
	// fakeStream with randomized outcome
	var fakeStream SimpleStream = func(_ context.Context) (Message, error) {
		if rand.Intn(10)%10 == 0 {
			// 1/10 chances to return fakeErr
			return nil, fakeErr
		}
		id := atomic.AddInt64(&msgID, 1)
		if rand.Intn(10)%10 == 1 {
			// 1/10 chances to sleep until circuit's timeout
			time.Sleep(DefaultCbTimeout + 10*time.Millisecond)
		}
		return SimpleMessage(strconv.FormatInt(id, 10)), nil
	}

	// resets all states(incl. metrics) of all circuits.
	CircuitReset()
	// create CircuitStream to protect fakeStream
	stream := NewCircuitStream(
		NewCircuitStreamOpts("", "test", xid.New().String()),
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

func ExampleNewCircuitStream_customCircuit() {
	rand.Seed(time.Now().UnixNano())
	fakeErr := errors.New("fake err")
	var msgID int64
	// fakeStream with randomized outcome
	var fakeStream SimpleStream = func(_ context.Context) (Message, error) {
		if rand.Intn(10)%10 == 0 {
			// 1/10 chances to return fakeErr
			return nil, fakeErr
		}
		id := atomic.AddInt64(&msgID, 1)
		if rand.Intn(10)%10 == 1 {
			// 1/10 chances to sleep until circuit's timeout
			time.Sleep(DefaultCbTimeout + 10*time.Millisecond)
		}
		return SimpleMessage(strconv.FormatInt(id, 10)), nil
	}

	// customize circuit's setting
	circuit := xid.New().String()
	conf := NewDefaultCircuitConf()
	conf.MaxConcurrent = 20
	conf.RegisterFor(circuit)
	// resets all states(incl. metrics) of all circuits.
	CircuitReset()
	// create CircuitStream to protect fakeStream
	stream := NewCircuitStream(
		NewCircuitStreamOpts("", "test", circuit),
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
