package gentle

import (
	"context"
	"errors"
	"github.com/benbjohnson/clock"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"math/rand"
	"sync"
	"testing"
	"testing/quick"
	"time"
)

func TestRateLimitedStream_Get(t *testing.T) {
	// Get() is rate limited.
	requestsInterval := 100 * time.Millisecond
	src, done := createInfiniteMessageChan()
	defer func() { done <- struct{}{} }()
	var chanStream SimpleStream = func(ctx2 context.Context) (Message, error) {
		return <-src, nil
	}
	stream := NewRateLimitedStream(
		NewRateLimitedStreamOpts("", "test",
			NewTokenBucketRateLimit(requestsInterval, 1)),
		chanStream)
	count := 4
	minimum := time.Duration(count-1) * requestsInterval
	var wg sync.WaitGroup
	wg.Add(count)
	begin := time.Now()
	for i := 0; i < count; i++ {
		go func() {
			_, err := stream.Get(context.Background())
			assert.NoError(t, err)
			wg.Done()
		}()
	}
	wg.Wait()
	dura := time.Since(begin)
	log.Info("[Test] spent >= minimum?", "spent", dura, "minimum", minimum)
	assert.True(t, dura >= minimum)
}

func TestRateLimitedStream_Get_Timeout(t *testing.T) {
	// Context timeout while Get() is waiting for rate-limiter or upstream.
	timeout := 100 * time.Millisecond
	run := func(intervalMs int) bool {
		requestsInterval := time.Duration(intervalMs) * time.Millisecond
		block := make(chan struct{}, 1)
		_, done := createInfiniteMessageChan()
		defer func() { done <- struct{}{} }()
		mstream := &MockStream{}
		mstream.On("Get", mock.Anything).Return((*SimpleMessage)(nil),
			func(ctx2 context.Context) error {
				select {
				case <-ctx2.Done():
					log.Debug("[test] Context.Done()", "err", ctx2.Err())
					return ctx2.Err()
				case <-block:
					panic("never here")
				}
			})
		stream := NewRateLimitedStream(
			NewRateLimitedStreamOpts("", "test",
				NewTokenBucketRateLimit(requestsInterval, 1)),
			mstream)
		count := 4
		var wg sync.WaitGroup
		wg.Add(count)
		begin := time.Now()
		for i := 0; i < count; i++ {
			go func() {
				ctx, _ := context.WithTimeout(context.Background(), timeout)
				_, err := stream.Get(ctx)
				if err == context.DeadlineExceeded {
					// It's interrupted when waiting either for the permission
					// from the rate-limiter or for mstream.Get()
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

func TestRetryStream_Get_MockBackOff(t *testing.T) {
	// Get() retries with mocked BackOff
	run := func(backOffCount int) bool {
		mfactory := &MockBackOffFactory{}
		mback := &MockBackOff{}
		mfactory.On("NewBackOff").Return(mback)
		// mock clock so that we don't need to wait for the real timer to move forward.
		mclock := clock.NewMock()
		opts := NewRetryStreamOpts("", "test", mfactory)
		opts.Clock = mclock
		mstream := &MockStream{}
		stream := NewRetryStream(opts, mstream)

		fakeErr := errors.New("A fake error")
		mstream.On("Get", mock.Anything).Return((*SimpleMessage)(nil), fakeErr)
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

func TestRetryStream_Get_ConstantBackOff(t *testing.T) {
	// Get() retries with ConstantBackOff
	run := func(maxElapsedSec int) bool {
		mclock := clock.NewMock()
		maxElapsedTime := time.Duration(maxElapsedSec) * time.Second
		backOffOpts := NewConstantBackOffFactoryOpts(time.Second, maxElapsedTime)
		backOffOpts.Clock = mclock
		backOffFactory := NewConstantBackOffFactory(backOffOpts)
		opts := NewRetryStreamOpts("", "test", backOffFactory)
		opts.Clock = mclock
		mstream := &MockStream{}
		stream := NewRetryStream(opts, mstream)

		fakeErr := errors.New("A fake error")
		mstream.On("Get", mock.Anything).Return((*SimpleMessage)(nil), fakeErr)

		ctx := context.Background()
		timespan := make(chan time.Duration, 1)
		go func() {
			begin := mclock.Now()
			_, err := stream.Get(ctx)
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

func TestRetryStream_Get_ExponentialBackOff(t *testing.T) {
	// Get() retries with ExponentialBackOff
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
		opts := NewRetryStreamOpts("", "test", backOffFactory)
		opts.Clock = mclock
		mstream := &MockStream{}
		stream := NewRetryStream(opts, mstream)

		fakeErr := errors.New("A fake error")
		mstream.On("Get", mock.Anything).Return((*SimpleMessage)(nil), fakeErr)

		ctx := context.Background()
		timespan := make(chan time.Duration, 1)
		go func() {
			begin := mclock.Now()
			_, err := stream.Get(ctx)
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

func TestRetryStream_Get_MockBackOff_Timeout(t *testing.T) {
	// Context timeout interrupts Get() with mocked BackOff
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
		opts := NewRetryStreamOpts("", "test", mfactory)
		mstream := &MockStream{}
		stream := NewRetryStream(opts, mstream)

		fakeErr := errors.New("A fake error")
		mstream.On("Get", mock.Anything).Return((*SimpleMessage)(nil),
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
		// [1ms, 2000ms)
		Values: genBoundInt(1, 2000),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestRetryStream_Get_ConstantBackOff_Timeout(t *testing.T) {
	// Context timeout interrupts Get() with ConstantBackOff
	suspend := 10 * time.Millisecond
	run := func(timeoutMs int) bool {
		backOffOpts := NewConstantBackOffFactoryOpts(suspend, 0)
		backOffFactory := NewConstantBackOffFactory(backOffOpts)
		timeout := time.Duration(timeoutMs) * time.Millisecond
		opts := NewRetryStreamOpts("", "test", backOffFactory)
		mstream := &MockStream{}
		stream := NewRetryStream(opts, mstream)

		fakeErr := errors.New("A fake error")
		mstream.On("Get", mock.Anything).Return((*SimpleMessage)(nil),
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
		// [1ms, 2000ms)
		Values: genBoundInt(1, 2000),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestRetryStream_Get_ExponentialBackOff_Timeout(t *testing.T) {
	// Context timeout interrupts Get() with ExponentialBackOff
	suspend := 10 * time.Millisecond
	run := func(timeoutMs int) bool {
		backOffOpts := NewExponentialBackOffFactoryOpts(suspend, 2,
			128*time.Second, 0)
		// No randomization to make the backoff time really exponential. Easier
		// to examine log.
		backOffOpts.RandomizationFactor = 0
		backOffFactory := NewExponentialBackOffFactory(backOffOpts)
		timeout := time.Duration(timeoutMs) * time.Millisecond
		opts := NewRetryStreamOpts("", "test", backOffFactory)
		mstream := &MockStream{}
		stream := NewRetryStream(opts, mstream)

		fakeErr := errors.New("A fake error")
		mstream.On("Get", mock.Anything).Return((*SimpleMessage)(nil),
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
		// [1ms, 2000ms)
		Values: genBoundInt(1, 2000),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestBulkheadStream_Get_MaxConcurrency(t *testing.T) {
	// BulkheadStream returns ErrMaxConcurrency when passing the threshold
	run := func(maxConcurrency int) bool {
		mstream := &MockStream{}
		stream := NewBulkheadStream(
			NewBulkheadStreamOpts("", "test", maxConcurrency),
			mstream)
		mm := SimpleMessage("123")

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
		Values: genBoundInt(1, 100),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestCircuitStream_Get_MaxConcurrency(t *testing.T) {
	// CircuitStream returns ErrCbMaxConcurrency when
	// CircuitConf.MaxConcurrent is reached.
	// It then returns ErrCbOpen when error threshold is reached.
	CircuitReset()
	circuit := xid.New().String()
	mstream := &MockStream{}

	conf := NewDefaultCircuitConf()
	conf.MaxConcurrent = 4
	// Set properly to not affect this test:
	conf.Timeout = time.Minute
	conf.SleepWindow = time.Minute
	// Set to quickly open the circuit
	conf.VolumeThreshold = 3
	conf.ErrorPercentThreshold = 20
	conf.RegisterFor(circuit)

	stream := NewCircuitStream(
		NewCircuitStreamOpts("", "test", circuit),
		mstream)
	mm := SimpleMessage("123")

	var wg sync.WaitGroup
	wg.Add(conf.MaxConcurrent)
	block := make(chan struct{}, 1)
	defer close(block)
	mstream.On("Get", mock.Anything).Return(
		func(ctx2 context.Context) Message {
			wg.Done()
			// block to saturate concurrent requests
			<-block
			return mm
		}, nil)

	ctx := context.Background()
	for i := 0; i < conf.MaxConcurrent; i++ {
		go func() {
			stream.Get(ctx)
		}()
	}
	// Make sure previous Get() are all running
	wg.Wait()

	for {
		_, err := stream.Get(ctx)
		if err == ErrCbOpen {
			log.Debug("[Test] circuit opened")
			break
		}
		assert.EqualError(t, err, ErrCbMaxConcurrency.Error())
	}
}

func TestCircuitStream_Get_Timeout(t *testing.T) {
	// CircuitStream returns ErrCbTimeout when
	// CircuitConf.Timeout is reached.
	// It then returns ErrCbOpen when error threshold is reached.
	CircuitReset()
	circuit := xid.New().String()
	mstream := &MockStream{}

	conf := NewDefaultCircuitConf()
	conf.Timeout = time.Millisecond
	// Set properly to not affect this test:
	conf.MaxConcurrent = 4096
	conf.SleepWindow = time.Minute
	// Set to quickly open the circuit
	conf.VolumeThreshold = 3
	conf.ErrorPercentThreshold = 20
	conf.RegisterFor(circuit)

	stream := NewCircuitStream(
		NewCircuitStreamOpts("", "test", circuit),
		mstream)
	mm := SimpleMessage("123")

	// Suspend longer than Timeout
	block := make(chan struct{}, 1)
	defer close(block)
	mstream.On("Get", mock.Anything).Return(
		func(ctx2 context.Context) Message {
			// block to hit timeout
			<-block
			return mm
		}, nil)

	ctx := context.Background()
	for {
		_, err := stream.Get(ctx)
		if err == ErrCbOpen {
			break
		}
		assert.EqualError(t, err, ErrCbTimeout.Error())
	}
}

func TestCircuitStream_Get_Error(t *testing.T) {
	// CircuitStream returns the designated error.
	// It then returns ErrCbOpen when error threshold is reached.
	CircuitReset()
	circuit := xid.New().String()
	mstream := &MockStream{}

	conf := NewDefaultCircuitConf()
	// Set properly to not affect this test:
	conf.MaxConcurrent = 4096
	conf.Timeout = time.Minute
	conf.SleepWindow = time.Minute
	// Set to quickly open the circuit
	conf.VolumeThreshold = 3
	conf.ErrorPercentThreshold = 20
	conf.RegisterFor(circuit)

	stream := NewCircuitStream(
		NewCircuitStreamOpts("", "test", circuit),
		mstream)
	fakeErr := errors.New("fake error")

	mstream.On("Get", mock.Anything).Return((*SimpleMessage)(nil), fakeErr)

	ctx := context.Background()
	for {
		_, err := stream.Get(ctx)
		if err == ErrCbOpen {
			break
		}
		assert.EqualError(t, err, fakeErr.Error())
	}
}

func TestStream_MsgIntact(t *testing.T) {
	// Resilient Streams don't modify msg from upstreams.
	msgOut := SimpleMessage("123")
	mstream := &MockStream{}
	mstream.On("Get", mock.Anything).Return(msgOut, nil)

	streams := []Stream{
		func() Stream {
			return NewRateLimitedStream(
				NewRateLimitedStreamOpts("", "test",
					NewTokenBucketRateLimit(time.Millisecond, 1)),
				mstream)
		}(),
		func() Stream {
			backOffOpts := NewConstantBackOffFactoryOpts(200*time.Millisecond, time.Second)
			backOffFactory := NewConstantBackOffFactory(backOffOpts)
			opts := NewRetryStreamOpts("", "test", backOffFactory)
			return NewRetryStream(opts, mstream)
		}(),
		func() Stream {
			backOffOpts := NewExponentialBackOffFactoryOpts(
				200*time.Millisecond, 2, time.Second, time.Second)
			backOffFactory := NewExponentialBackOffFactory(backOffOpts)
			opts := NewRetryStreamOpts("", "test", backOffFactory)
			return NewRetryStream(opts, mstream)
		}(),
		func() Stream {
			return NewBulkheadStream(
				NewBulkheadStreamOpts("", "test", 1024),
				mstream)
		}(),
		func() Stream {
			return NewCircuitStream(
				NewCircuitStreamOpts("", "test", xid.New().String()),
				mstream)
		}(),
	}

	for _, stream := range streams {
		msg, _ := stream.Get(context.Background())
		assert.Equal(t, msgOut.ID(), msg.ID())
	}
}
