package gentle

import (
	"context"
	"errors"
	"github.com/benbjohnson/clock"
	"github.com/cfchou/hystrix-go/hystrix"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"math/rand"
	"sync"
	"testing"
	"testing/quick"
	"time"
)

func TestRateLimitedHandler_Handle(t *testing.T) {
	// Handle() is rate limited.
	requestsInterval := 100 * time.Millisecond
	mhandler := &MockHandler{}
	handler := NewRateLimitedHandler(
		NewRateLimitedHandlerOpts("", "test",
			NewTokenBucketRateLimit(requestsInterval, 1)),
		mhandler)
	mm := &fakeMsg{id: "123"}
	mhandler.On("Handle", mock.Anything, mock.Anything).Return(mm, nil)
	count := 4
	minimum := time.Duration(count-1) * requestsInterval
	var wg sync.WaitGroup
	wg.Add(count)
	begin := time.Now()
	ctx := context.Background()
	for i := 0; i < count; i++ {
		go func() {
			_, err := handler.Handle(ctx, mm)
			assert.NoError(t, err)
			wg.Done()
		}()
	}
	wg.Wait()
	dura := time.Now().Sub(begin)
	log.Info("[Test] spent >= minmum?", "spent", dura, "minimum", minimum)
	assert.True(t, dura >= minimum)
}

func TestRateLimitedHandler_Handle_Timeout(t *testing.T) {
	// Context timeout while Handle() is waiting for rate-limiter or upstream.
	timeout := 100 * time.Millisecond
	run := func(intervalMs int) bool {
		requestsInterval := time.Duration(intervalMs) * time.Millisecond
		block := make(chan struct{}, 1)
		mhandler := &MockHandler{}
		mhandler.On("Handle", mock.Anything, mock.Anything).Return(
			(*fakeMsg)(nil), func(ctx2 context.Context, _ Message) error {
				select {
				case <-ctx2.Done():
					log.Debug("[test] Context.Done()", "err", ctx2.Err())
					return ctx2.Err()
				case <-block:
					panic("never here")
				}
			})
		stream := NewRateLimitedHandler(
			NewRateLimitedHandlerOpts("", "test",
				NewTokenBucketRateLimit(requestsInterval, 1)),
			mhandler)
		mm := &fakeMsg{id: "123"}
		count := 4
		var wg sync.WaitGroup
		wg.Add(count)
		begin := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		for i := 0; i < count; i++ {
			go func() {
				_, err := stream.Handle(ctx, mm)
				if err == context.DeadlineExceeded {
					// It's interrupted when waiting either for the permission
					// from the rate-limiter or for mhandler.Handle()
					wg.Done()
					return
				}
				panic("never here")
			}()
		}
		wg.Wait()
		log.Info("[Test] time spent", "timespan", time.Since(begin).Seconds())
		return true
	}
	config := &quick.Config{
		// [1ms, 200ms)
		Values: genBoundInt(1, 200),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestRetryHandler_Handle_MockBackOff(t *testing.T) {
	// Handle() retries with mocked BackOff
	run := func(backOffCount int) bool {
		mfactory := &MockBackOffFactory{}
		mback := &MockBackOff{}
		mfactory.On("NewBackOff").Return(mback)
		// mock clock so that we don't need to wait for the real timer to move forward.
		mclock := clock.NewMock()
		opts := NewRetryHandlerOpts("", "test", mfactory)
		opts.Clock = mclock
		mhandler := &MockHandler{}
		handler := NewRetryHandler(opts, mhandler)

		fakeErr := errors.New("A fake error")
		mhandler.On("Handle", mock.Anything, mock.Anything).Return((*fakeMsg)(nil), fakeErr)
		// create a backoff that fires 1 second for $backOffCount times
		timespanMinimum := time.Duration(backOffCount) * time.Second
		mback.On("Next").Return(func() time.Duration {
			if backOffCount == 0 {
				return BackOffStop
			}
			backOffCount--
			return 1 * time.Second
		})

		ctx := context.Background()
		mm := &fakeMsg{}
		timespan := make(chan time.Duration, 1)
		go func() {
			begin := mclock.Now()
			_, err := handler.Handle(ctx, mm)
			// back-offs exhausted
			if err != fakeErr {
				panic("never here")
			}
			timespan <- mclock.Now().Sub(begin)
		}()

		for {
			select {
			case dura := <-timespan:
				log.Info("[Test] spent >= minmum?", "spent", dura, "timespanMinimum", timespanMinimum)
				return dura >= timespanMinimum
			default:
				// advance an arbitrary time to pass all backoffs
				mclock.Add(1 * time.Second)
			}
		}
	}

	config := &quick.Config{
		Values: genBoundInt(1, 50),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestRetryHandler_Handle_ConstantBackOff(t *testing.T) {
	// Handle() retries with ConstantBackOff
	run := func(maxElapsedSec int) bool {
		mclock := clock.NewMock()
		maxElapsedTime := time.Duration(maxElapsedSec) * time.Second
		backOffOpts := NewConstantBackOffFactoryOpts(time.Second, maxElapsedTime)
		backOffOpts.Clock = mclock
		backOffFactory := NewConstantBackOffFactory(backOffOpts)
		opts := NewRetryHandlerOpts("", "test", backOffFactory)
		opts.Clock = mclock
		mhandler := &MockHandler{}
		handler := NewRetryHandler(opts, mhandler)

		fakeErr := errors.New("A fake error")
		mhandler.On("Handle", mock.Anything, mock.Anything).
			Return((*fakeMsg)(nil), fakeErr)

		ctx := context.Background()
		mm := &fakeMsg{}
		timespan := make(chan time.Duration, 1)
		go func() {
			begin := mclock.Now()
			_, err := handler.Handle(ctx, mm)
			// back-offs exhausted
			if err != fakeErr {
				panic("never here")
			}
			timespan <- mclock.Now().Sub(begin)
		}()

		for {
			select {
			case dura := <-timespan:
				log.Info("[Test] spent >= minmum?", "spent", dura, "maxElapsedTime", maxElapsedTime)
				return dura >= maxElapsedTime
			default:
				// advance an arbitrary time to pass all backoffs
				mclock.Add(1 * time.Second)
			}
		}
	}
	config := &quick.Config{
		Values: genBoundInt(1, 50),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestRetryHandler_Handle_ExponentialBackOff(t *testing.T) {
	// Handle() retries with ExponentialBackOff
	run := func(maxElapsedSec int) bool {
		mclock := clock.NewMock()
		maxElapsedTime := time.Duration(maxElapsedSec) * time.Second
		backOffOpts := NewExponentialBackOffFactoryOpts(time.Second, 2,
			128*time.Second, maxElapsedTime)
		// No randomization to make the backoff time really exponential. Easier
		// to examine log.
		backOffOpts.RandomizationFactor = 0
		backOffOpts.Clock = mclock
		backOffFactory := NewExponentialBackOffFactory(backOffOpts)
		opts := NewRetryHandlerOpts("", "test", backOffFactory)
		opts.Clock = mclock
		mhandler := &MockHandler{}
		handler := NewRetryHandler(opts, mhandler)

		fakeErr := errors.New("A fake error")
		mhandler.On("Handle", mock.Anything, mock.Anything).
			Return((*fakeMsg)(nil), fakeErr)

		ctx := context.Background()
		mm := &fakeMsg{id: "123"}
		timespan := make(chan time.Duration, 1)
		go func() {
			begin := mclock.Now()
			_, err := handler.Handle(ctx, mm)
			// back-offs exhausted
			if err != fakeErr {
				panic("never here")
			}
			timespan <- mclock.Now().Sub(begin)
		}()

		for {
			select {
			case dura := <-timespan:
				log.Info("[Test] spent >= minmum?", "spent", dura, "maxElapsedTime", maxElapsedTime)
				return dura >= maxElapsedTime
			default:
				// advance an arbitrary time to pass all backoffs
				mclock.Add(1 * time.Second)
			}
		}
	}
	config := &quick.Config{
		MaxCount: 10,
		// [1s, 20m)
		Values: genBoundInt(1, 1200),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestRetryHandler_Handle_MockBackOff_Timeout(t *testing.T) {
	// Context timeout interrupts Handle() with mocked BackOff
	suspend := 10 * time.Millisecond
	run := func(timeoutMs int) bool {
		mfactory := &MockBackOffFactory{}
		mback := &MockBackOff{}
		timeout := time.Duration(timeoutMs) * time.Millisecond
		mfactory.On("NewBackOff").Return(func() BackOff {
			// 1/10 chances that NewBackOff() would sleep over timeout
			if rand.Intn(timeoutMs)%10 == 1 {
				time.Sleep(timeout + 10*time.Second)
			}
			return mback
		})
		opts := NewRetryHandlerOpts("", "test", mfactory)
		mhandler := &MockHandler{}
		handler := NewRetryHandler(opts, mhandler)

		fakeErr := errors.New("A fake error")
		mhandler.On("Handle", mock.Anything, mock.Anything).
			Return((*fakeMsg)(nil), func(ctx2 context.Context, _ Message) error {
				log.Debug("[test] Handle()...")
				tm := time.NewTimer(suspend)
				select {
				case <-ctx2.Done():
					tm.Stop()
					err := ctx2.Err()
					log.Debug("[test] interrupted", "err", err)
					return err
				case <-tm.C:
				}
				return fakeErr
			})
		// Never run out of back-offs
		mback.On("Next").Return(func() time.Duration {
			log.Debug("[test] Next() sleep")
			time.Sleep(suspend)
			return suspend
		})

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		mm := &fakeMsg{id: "123"}

		_, err := handler.Handle(ctx, mm)
		return err == context.DeadlineExceeded
	}

	config := &quick.Config{
		MaxCount: 10,
		// [1ms, 2000ms)
		Values: genBoundInt(1, 2000),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestRetryHandler_Handle_ConstantBackOff_Timeout(t *testing.T) {
	// Context timeout interrupts Handle() with ConstantBackOff
	suspend := 10 * time.Millisecond
	run := func(timeoutMs int) bool {
		backOffOpts := NewConstantBackOffFactoryOpts(suspend, 0)
		backOffFactory := NewConstantBackOffFactory(backOffOpts)
		timeout := time.Duration(timeoutMs) * time.Millisecond
		opts := NewRetryHandlerOpts("", "test", backOffFactory)
		mhandler := &MockHandler{}
		handler := NewRetryHandler(opts, mhandler)

		fakeErr := errors.New("A fake error")
		mhandler.On("Handle", mock.Anything, mock.Anything).
			Return((*fakeMsg)(nil), func(ctx2 context.Context, _ Message) error {
				log.Debug("[test] Handle()...")
				tm := time.NewTimer(suspend)
				select {
				case <-ctx2.Done():
					tm.Stop()
					err := ctx2.Err()
					log.Debug("[test] interrupted", "err", err)
					return err
				case <-tm.C:
				}
				return fakeErr
			})
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		mm := &fakeMsg{id: "123"}

		_, err := handler.Handle(ctx, mm)
		return err == context.DeadlineExceeded
	}

	config := &quick.Config{
		MaxCount: 10,
		// [1ms, 2000ms)
		Values: genBoundInt(1, 2000),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestRetryHandler_Handle_ExponentialBackOff_Timeout(t *testing.T) {
	// Context timeout interrupts Handle() with ExponentialBackOff
	suspend := 10 * time.Millisecond
	run := func(timeoutMs int) bool {
		backOffOpts := NewExponentialBackOffFactoryOpts(suspend, 2,
			128*time.Second, 0)
		// No randomization to make the backoff time really exponential. Easier
		// to examine log.
		backOffOpts.RandomizationFactor = 0
		backOffFactory := NewExponentialBackOffFactory(backOffOpts)
		timeout := time.Duration(timeoutMs) * time.Millisecond
		opts := NewRetryHandlerOpts("", "test", backOffFactory)
		mhandler := &MockHandler{}
		handler := NewRetryHandler(opts, mhandler)

		fakeErr := errors.New("A fake error")
		mhandler.On("Handle", mock.Anything, mock.Anything).
			Return((*fakeMsg)(nil), func(ctx2 context.Context, _ Message) error {
				log.Debug("[test] Handle()...")
				tm := time.NewTimer(suspend)
				select {
				case <-ctx2.Done():
					tm.Stop()
					err := ctx2.Err()
					log.Debug("[test] interrupted", "err", err)
					return err
				case <-tm.C:
				}
				return fakeErr
			})
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		mm := &fakeMsg{id: "123"}

		_, err := handler.Handle(ctx, mm)
		return err == context.DeadlineExceeded
	}

	config := &quick.Config{
		MaxCount: 10,
		// [1ms, 2000ms)
		Values: genBoundInt(1, 2000),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestBulkheadHandler_Handle_MaxConcurrency(t *testing.T) {
	// BulkheadHandler returns ErrMaxConcurrency when passing the threshold
	run := func(maxConcurrency int) bool {
		mhandler := &MockHandler{}
		handler := NewBulkheadHandler(
			NewBulkheadHandlerOpts("", "test", maxConcurrency),
			mhandler)
		wg := &sync.WaitGroup{}
		wg.Add(maxConcurrency)
		block := make(chan struct{}, 1)
		defer close(block)
		mhandler.On("Handle", mock.Anything, mock.Anything).Return(
			func(ctx2 context.Context, m Message) Message {
				wg.Done()
				// every Handle() would be blocked here
				<-block
				return m
			}, nil)

		ctx := context.Background()
		mm := &fakeMsg{id: "123"}

		for i := 0; i < maxConcurrency; i++ {
			go handler.Handle(ctx, mm)
		}

		// Wait() until $maxConcurrency of Handle() are blocked
		wg.Wait()
		// one more Handle() would cause ErrMaxConcurrency
		msg, err := handler.Handle(ctx, mm)
		return msg == nil && err == ErrMaxConcurrency
	}

	config := &quick.Config{
		Values: genBoundInt(1, 100),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestCircuitBreakerHandler_Handle_MaxConcurrency(t *testing.T) {
	// CircuitBreakerHandler returns ErrCbMaxConcurrency when
	// CircuitBreakerConf.MaxConcurrent is reached.
	// It then returns ErrCbOpen when error threshold is reached.
	defer hystrix.Flush()
	circuit := xid.New().String()
	mhandler := &MockHandler{}

	conf := NewDefaultCircuitBreakerConf()
	conf.MaxConcurrent = 4
	// Set properly to not affect this test:
	conf.Timeout = time.Minute
	conf.SleepWindow = time.Minute
	// Set to quickly open the circuit
	conf.VolumeThreshold = 3
	conf.ErrorPercentThreshold = 20
	conf.RegisterFor(circuit)

	handler := NewCircuitBreakerHandler(
		NewCircuitBreakerHandlerOpts("", "test", circuit),
		mhandler)
	var wg sync.WaitGroup
	wg.Add(conf.MaxConcurrent)
	block := make(chan struct{}, 1)
	defer close(block)
	mhandler.On("Handle", mock.Anything, mock.Anything).Return(
		func(ctx2 context.Context, msg Message) Message {
			wg.Done()
			// block to saturate concurrent requests
			<-block
			return msg
		}, nil)

	ctx := context.Background()
	mm := &fakeMsg{id: "123"}

	for i := 0; i < conf.MaxConcurrent; i++ {
		go func() {
			handler.Handle(ctx, mm)
		}()
	}
	// Make sure previous Get() are all running
	wg.Wait()

	for {
		_, err := handler.Handle(ctx, mm)
		if err == ErrCbOpen {
			log.Debug("[Test] circuit opened")
			break
		}
		assert.EqualError(t, err, ErrCbMaxConcurrency.Error())
	}
}

func TestCircuitBreakerHandler_Handle_Timeout(t *testing.T) {
	// CircuitBreakerHandler returns ErrCbTimeout when
	// CircuitBreakerConf.Timeout is reached.
	// It then returns ErrCbOpen when error threshold is reached.
	defer hystrix.Flush()
	circuit := xid.New().String()
	mhandler := &MockHandler{}

	conf := NewDefaultCircuitBreakerConf()
	conf.Timeout = time.Millisecond
	// Set properly to not affect this test:
	conf.MaxConcurrent = 4096
	conf.SleepWindow = time.Minute
	// Set to quickly open the circuit
	conf.VolumeThreshold = 3
	conf.ErrorPercentThreshold = 20
	conf.RegisterFor(circuit)

	handler := NewCircuitBreakerHandler(
		NewCircuitBreakerHandlerOpts("", "test", circuit),
		mhandler)

	// Suspend longer than Timeout
	block := make(chan struct{}, 1)
	defer close(block)
	mhandler.On("Handle", mock.Anything, mock.Anything).Return(
		func(ctx2 context.Context, msg Message) Message {
			// block to hit timeout
			<-block
			return msg
		}, nil)

	ctx := context.Background()
	mm := &fakeMsg{id: "123"}
	for {
		_, err := handler.Handle(ctx, mm)
		if err == ErrCbOpen {
			break
		}
		assert.EqualError(t, err, ErrCbTimeout.Error())
	}
}

func TestCircuitBreakerHandler_Handle_Error(t *testing.T) {
	// CircuitBreakerHandler returns the designated error.
	// It then returns ErrCbOpen when error threshold is reached.
	defer hystrix.Flush()
	circuit := xid.New().String()
	mhandler := &MockHandler{}

	conf := NewDefaultCircuitBreakerConf()
	// Set properly to not affect this test:
	conf.MaxConcurrent = 4096
	conf.Timeout = time.Minute
	conf.SleepWindow = time.Minute
	// Set to quickly open the circuit
	conf.VolumeThreshold = 3
	conf.ErrorPercentThreshold = 20
	conf.RegisterFor(circuit)

	handler := NewCircuitBreakerHandler(
		NewCircuitBreakerHandlerOpts("", "test", circuit),
		mhandler)
	fakeErr := errors.New("fake error")

	mhandler.On("Handle", mock.Anything, mock.Anything).Return((*fakeMsg)(nil), fakeErr)

	ctx := context.Background()
	mm := &fakeMsg{id: "123"}
	for {
		_, err := handler.Handle(ctx, mm)
		if err == ErrCbOpen {
			break
		}
		assert.EqualError(t, err, fakeErr.Error())
	}
}
