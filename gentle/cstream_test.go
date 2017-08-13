package gentle

import (
	"context"
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/benbjohnson/clock"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"testing/quick"
	"time"
)

func genBoundNonNegInt(min, max int) func(values []reflect.Value, rnd *rand.Rand) {
	if min < 0 || max < 0 {
		panic("Invalid argument")
	}
	return func(values []reflect.Value, rnd *rand.Rand) {
		for {
			// Within an hour
			n := rnd.Intn(max + 1)
			if n < min {
				continue
			}
			values[0] = reflect.ValueOf(n)
			break
		}
	}
}

func TestRateLimitedCStream_Get(t *testing.T) {
	requestsInterval := 100 * time.Millisecond
	src, done := createInfiniteMessageChan()
	defer func() { done <- struct{}{} }()
	var chanStream SimpleCStream = func(ctx2 context.Context) (Message, error) {
		return <-src, nil
	}
	stream := NewRateLimitedCStream(
		NewRateLimitedCStreamOpts("", "test",
			NewTokenBucketRateLimit(requestsInterval, 1)),
		chanStream)
	count := 4
	minimum := time.Duration(count-1) * requestsInterval
	var wg sync.WaitGroup
	wg.Add(count)
	begin := time.Now()
	ctx := context.Background()
	for i := 0; i < count; i++ {
		go func() {
			_, err := stream.Get(ctx)
			assert.NoError(t, err)
			wg.Done()
		}()
	}
	wg.Wait()
	dura := time.Now().Sub(begin)
	log.Info("[Test] spent >= minimum?", "spent", dura, "minimum", minimum)
	assert.True(t, dura >= minimum)
}

func TestRateLimitedCStream_Get_Timeout(t *testing.T) {
	// Get() timeout, while it's waiting for rate-limiter or upstream
	run := func(intervalMs int) bool {
		requestsInterval := time.Duration(intervalMs) * time.Millisecond
		timeout := 100 * time.Millisecond
		block := make(chan struct{}, 1)
		_, done := createInfiniteMessageChan()
		defer func() { done <- struct{}{} }()
		var chanStream SimpleCStream = func(ctx2 context.Context) (Message, error) {
			select {
			case <-ctx2.Done():
				log.Debug("[test] Context.Done()", "err", ctx2.Err())
				return nil, ctx2.Err()
			case <-block:
				panic("never here")
			}
		}
		stream := NewRateLimitedCStream(
			NewRateLimitedCStreamOpts("", "test",
				NewTokenBucketRateLimit(requestsInterval, 1)),
			chanStream)
		count := 4
		var wg sync.WaitGroup
		wg.Add(count)
		begin := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		success, failure := make(chan struct{}, 1), make(chan struct{}, count)
		for i := 0; i < count; i++ {
			go func() {
				_, err := stream.Get(ctx)
				if err != context.DeadlineExceeded {
					failure <- struct{}{}
					return
				}
				wg.Done()
			}()
		}
		go func() {
			wg.Wait()
			success <- struct{}{}
		}()
		select {
		case <-success:
			log.Info("[Test] time spent",
				"timespan", time.Since(begin).Seconds())
			return true
		case <-failure:
			return false
		}
	}
	config := &quick.Config{
		// [1ms, 1hour]
		Values: genBoundNonNegInt(1, 3600*1000),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestRetryCStream_Get_MockBackOff(t *testing.T) {
	// Test against mocked BackOff
	run := func(backOffCount int) bool {
		mfactory := &MockBackOffFactory{}
		mback := &MockBackOff{}
		mfactory.On("NewBackOff").Return(mback)
		// mock clock so that we don't need to wait for the real timer to move forward.
		mclock := clock.NewMock()
		opts := NewRetryCStreamOpts("", "test", mfactory)
		opts.Clock = mclock
		mstream := &MockCStream{}
		stream := NewRetryCStream(opts, mstream)

		fakeErr := errors.New("A fake error")
		mstream.On("Get", mock.Anything).Return((*fakeMsg)(nil), fakeErr)
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
		timespan := make(chan time.Duration, 1)
		go func() {
			begin := mclock.Now()
			_, err := stream.Get(ctx)
			// backoffs exhausted
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
		Values: genBoundNonNegInt(1, 50),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestRetryCStream_Get_ConstantBackOff(t *testing.T) {
	// Test against ConstantBackOff
	run := func(maxElapsedSec int) bool {
		mclock := clock.NewMock()
		maxElapsedTime := time.Duration(maxElapsedSec) * time.Second
		backOffOpts := NewConstantBackOffFactoryOpts(time.Second, maxElapsedTime)
		backOffOpts.Clock = mclock
		backOffFactory := NewConstantBackOffFactory(backOffOpts)
		opts := NewRetryCStreamOpts("", "test", backOffFactory)
		opts.Clock = mclock
		mstream := &MockCStream{}
		stream := NewRetryCStream(opts, mstream)

		fakeErr := errors.New("A fake error")
		mstream.On("Get", mock.Anything).Return((*fakeMsg)(nil), fakeErr)

		ctx := context.Background()
		timespan := make(chan time.Duration, 1)
		go func() {
			begin := mclock.Now()
			_, err := stream.Get(ctx)
			// backoffs exhausted
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
		Values: genBoundNonNegInt(1, 50),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestRetryCStream_Get_ExponentialBackOff(t *testing.T) {
	// Test against ExponentialBackOff
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
		opts := NewRetryCStreamOpts("", "test", backOffFactory)
		opts.Clock = mclock
		mstream := &MockCStream{}
		stream := NewRetryCStream(opts, mstream)

		fakeErr := errors.New("A fake error")
		mstream.On("Get", mock.Anything).Return((*fakeMsg)(nil), fakeErr)

		ctx := context.Background()
		timespan := make(chan time.Duration, 1)
		go func() {
			begin := mclock.Now()
			_, err := stream.Get(ctx)
			// backoffs exhausted
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
		// [1s, 20m]
		Values: genBoundNonNegInt(1, 1200),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestRetryCStream_Get_MockBackOff_Timeout(t *testing.T) {
	// Timeout interrupt mocked BackOff
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
		opts := NewRetryCStreamOpts("", "test", mfactory)
		mstream := &MockCStream{}
		stream := NewRetryCStream(opts, mstream)

		fakeErr := errors.New("A fake error")
		mstream.On("Get", mock.Anything).Return((*fakeMsg)(nil),
			func(ctx2 context.Context) error {
				log.Debug("[test] Get()...")
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

		_, err := stream.Get(ctx)
		return err == context.DeadlineExceeded
	}

	config := &quick.Config{
		MaxCount: 10,
		// [1ms, 2000ms]
		Values: genBoundNonNegInt(1, 2000),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestRetryCStream_Get_ConstantBackOff_Timeout(t *testing.T) {
	// Timeout interrupt ConstantBackOff
	suspend := 10 * time.Millisecond
	run := func(timeoutMs int) bool {
		backOffOpts := NewConstantBackOffFactoryOpts(suspend, 0)
		backOffFactory := NewConstantBackOffFactory(backOffOpts)
		timeout := time.Duration(timeoutMs) * time.Millisecond
		opts := NewRetryCStreamOpts("", "test", backOffFactory)
		mstream := &MockCStream{}
		stream := NewRetryCStream(opts, mstream)

		fakeErr := errors.New("A fake error")
		mstream.On("Get", mock.Anything).Return((*fakeMsg)(nil),
			func(ctx2 context.Context) error {
				log.Debug("[test] Get()...")
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

		_, err := stream.Get(ctx)
		return err == context.DeadlineExceeded
	}

	config := &quick.Config{
		MaxCount: 10,
		// [1ms, 2000ms]
		Values: genBoundNonNegInt(1, 2000),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestRetryCStream_Get_ExponentialBackOff_Timeout(t *testing.T) {
	// Timeout interrupt ExponentialBackOff
	suspend := 10 * time.Millisecond
	run := func(timeoutMs int) bool {
		backOffOpts := NewExponentialBackOffFactoryOpts(suspend, 2,
			128*time.Second, 0)
		// No randomization to make the backoff time really exponential. Easier
		// to examine log.
		backOffOpts.RandomizationFactor = 0
		backOffFactory := NewExponentialBackOffFactory(backOffOpts)
		timeout := time.Duration(timeoutMs) * time.Millisecond
		opts := NewRetryCStreamOpts("", "test", backOffFactory)
		mstream := &MockCStream{}
		stream := NewRetryCStream(opts, mstream)

		fakeErr := errors.New("A fake error")
		mstream.On("Get", mock.Anything).Return((*fakeMsg)(nil),
			func(ctx2 context.Context) error {
				log.Debug("[test] Get()...")
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

		_, err := stream.Get(ctx)
		return err == context.DeadlineExceeded
	}

	config := &quick.Config{
		MaxCount: 10,
		// [1ms, 2000ms]
		Values: genBoundNonNegInt(1, 2000),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestBulkheadCStream_Get_MaxConcurrency(t *testing.T) {
	// Return ErrMaxConcurrency when allowance is reached
	run := func(maxConcurrency int) bool {
		mstream := &MockCStream{}
		stream := NewBulkheadCStream(
			NewBulkheadCStreamOpts("", "test", maxConcurrency),
			mstream)
		mm := &fakeMsg{id: "123"}

		wg := &sync.WaitGroup{}
		wg.Add(maxConcurrency)
		block := make(chan struct{}, 1)
		defer close(block)
		mstream.On("Get", mock.Anything).Return(
			func(ctx2 context.Context) Message {
				wg.Done()
				// every Get() would be blocked here
				<-block
				return mm
			}, nil)

		ctx := context.Background()
		for i := 0; i < maxConcurrency; i++ {
			go stream.Get(ctx)
		}

		// Wait() until $maxConcurrency of Get() are blocked
		wg.Wait()
		// one more Get() would cause ErrMaxConcurrency
		msg, err := stream.Get(ctx)
		return msg == nil && err == ErrMaxConcurrency
	}

	config := &quick.Config{
		MaxCount: 10,
		Values:   genBoundNonNegInt(1, 1000),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

// TODO: fix race-condition
func __TestCircuitBreakerCStream_Get_MaxConcurrency(t *testing.T) {
	// Return ErrCbMaxConcurrency when allowance is reached.
	//
	defer hystrix.Flush()
	maxConcurrency := 4
	circuit := xid.New().String()
	mstream := &MockCStream{}

	conf := NewDefaultCircuitBreakerConf()
	conf.MaxConcurrent = maxConcurrency
	// Make Timeout large enough for this test case
	conf.Timeout = time.Minute
	// The number of errors of ErrCbMaxConcurrency makes up ErrCbOpen
	conf.VolumeThreshold = 5
	conf.ErrorPercentThreshold = 10
	// During SleepWindow, ErrCbOpen remains.
	conf.SleepWindow = time.Minute
	conf.RegisterFor(circuit)

	stream := NewCircuitBreakerCStream(
		NewCircuitBreakerCStreamOpts("", "test", circuit),
		mstream)
	mm := &fakeMsg{id: "123"}

	var wg sync.WaitGroup
	wg.Add(maxConcurrency)
	block := make(chan struct{}, 1)
	defer close(block)
	mstream.On("Get", mock.Anything).Return(
		func(ctx2 context.Context) Message {
			wg.Done()
			<-block
			return mm
		}, nil)

	ctx := context.Background()
	for i := 0; i < maxConcurrency; i++ {
		go func() {
			stream.Get(ctx)
		}()
	}
	// Make sure previous Get() are all running before the next
	wg.Wait()
	// One more call while all previous calls sticking in the circuit
	_, err := stream.Get(ctx)
	assert.EqualError(t, err, ErrCbMaxConcurrency.Error())

	for {
		_, err := stream.Get(ctx)
		if err == ErrCbOpen {
			log.Debug("[Test] circuit opened")
			break
		}
		assert.EqualError(t, err, ErrCbMaxConcurrency.Error())
	}
}
