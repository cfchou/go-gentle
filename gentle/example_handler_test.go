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

func ExampleSimpleHandler() {
	var handler SimpleHandler = func(_ context.Context, msg Message) (Message, error) {
		return msg, nil
	}

	msg, _ := handler.Handle(context.Background(), SimpleMessage("abc"))
	fmt.Println("msg:", msg.ID())
	// Output:
	// msg: abc
}

func ExampleNewRateLimitedHandler() {
	var fakeHandler SimpleHandler = func(_ context.Context, msg Message) (Message, error) {
		return msg, nil
	}

	count := 5
	interval := 100 * time.Millisecond
	minimum := time.Duration(count-1) * interval

	// limit the rate to access fakeStream
	handler := NewRateLimitedHandler(
		NewRateLimitedHandlerOpts("", "test",
			NewTokenBucketRateLimit(interval, 1)),
		fakeHandler)

	begin := time.Now()
	wg := &sync.WaitGroup{}
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			handler.Handle(context.Background(), SimpleMessage("abc"))
		}()
	}
	wg.Wait()
	fmt.Printf("Spend more than %s? %t\n", minimum,
		time.Now().After(begin.Add(minimum)))
	// Output: Spend more than 400ms? true
}

func ExampleNewRetryHandler_contantBackOff() {
	fakeErr := errors.New("fake err")
	// fakeHandler keeps triggering back-offs.
	var fakeHandler SimpleHandler = func(_ context.Context, _ Message) (Message, error) {
		return nil, fakeErr
	}

	// No more back-off when total execution + back-offs elapsed more than 1s.
	backOffOpts := NewConstBackOffFactoryOpts(100*time.Millisecond, time.Second)
	backOffFactory := NewConstBackOffFactory(backOffOpts)
	opts := NewRetryHandlerOpts("", "test", backOffFactory)
	// Retry with back-offs to access fakeHandler
	handler := NewRetryHandler(opts, fakeHandler)
	_, err := handler.Handle(context.Background(), SimpleMessage("abc"))
	fmt.Println(err)
	// Output: fake err
}

func ExampleNewRetryHandler_expBackOff() {
	fakeErr := errors.New("fake err")
	// fakeHandler keeps triggering back-offs.
	var fakeHandler SimpleHandler = func(_ context.Context, _ Message) (Message, error) {
		return nil, fakeErr
	}

	// No more back-off when total execution + back-offs elapsed more than 2s.
	backOffOpts := NewExpBackOffFactoryOpts(100*time.Millisecond, 2,
		time.Second, 2*time.Second)
	backOffFactory := NewExpBackOffFactory(backOffOpts)
	opts := NewRetryHandlerOpts("", "test", backOffFactory)
	// Retry with back-offs to access fakeHandler
	handler := NewRetryHandler(opts, fakeHandler)
	_, err := handler.Handle(context.Background(), SimpleMessage("abc"))
	fmt.Println(err)
	// Output: fake err
}

func ExampleNewBulkheadHandler() {
	var fakeHandler SimpleHandler = func(_ context.Context, msg Message) (Message, error) {
		return msg, nil
	}

	count := 10
	// limit concurrent access to fakeHandler
	handler := NewBulkheadHandler(
		NewBulkheadHandlerOpts("", "test", 2),
		fakeHandler)

	wg := &sync.WaitGroup{}
	wg.Add(count)
	var msgID int64
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			id := atomic.AddInt64(&msgID, 1)
			msg, err := handler.Handle(context.Background(),
				SimpleMessage(strconv.FormatInt(id, 10)))
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

func ExampleNewCircuitHandler() {
	rand.Seed(time.Now().UnixNano())
	fakeErr := errors.New("fake err")
	// fakeHandler with randomized outcome
	var fakeHandler SimpleHandler = func(_ context.Context, msg Message) (Message, error) {
		if rand.Intn(10)%10 == 0 {
			// 1/10 chances to return fakeErr
			return nil, fakeErr
		}
		if rand.Intn(10)%10 == 1 {
			// 1/10 chances to sleep until circuit's timeout
			time.Sleep(DefaultCbTimeout + 10*time.Millisecond)
		}
		return msg, nil
	}

	// resets all states(incl. metrics) of all circuits.
	CircuitReset()
	// create CircuitHandler to protect fakeHandler
	handler := NewCircuitHandler(
		NewCircuitHandlerOpts("", "test", xid.New().String()),
		fakeHandler)

	count := 100
	wg := &sync.WaitGroup{}
	wg.Add(count)
	var msgID int64
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			id := atomic.AddInt64(&msgID, 1)
			msg, err := handler.Handle(context.Background(),
				SimpleMessage(strconv.FormatInt(id, 10)))
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

func ExampleNewCircuitHandler_customCircuit() {
	rand.Seed(time.Now().UnixNano())
	fakeErr := errors.New("fake err")
	// fakeHandler with randomized outcome
	var fakeHandler SimpleHandler = func(_ context.Context, msg Message) (Message, error) {
		if rand.Intn(10)%10 == 0 {
			// 1/10 chances to return fakeErr
			return nil, fakeErr
		}
		if rand.Intn(10)%10 == 1 {
			// 1/10 chances to sleep until circuit's timeout
			time.Sleep(DefaultCbTimeout + 10*time.Millisecond)
		}
		return msg, nil
	}

	// customize circuit's setting
	circuit := xid.New().String()
	conf := NewDefaultCircuitConf()
	conf.MaxConcurrent = 20
	conf.RegisterFor(circuit)
	// resets all states(incl. metrics) of all circuits.
	CircuitReset()
	// create CircuitHandler to protect fakeHandler
	handler := NewCircuitHandler(
		NewCircuitHandlerOpts("", "test", circuit),
		fakeHandler)

	count := 100
	wg := &sync.WaitGroup{}
	wg.Add(count)
	var msgID int64
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			id := atomic.AddInt64(&msgID, 1)
			msg, err := handler.Handle(context.Background(),
				SimpleMessage(strconv.FormatInt(id, 10)))
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
