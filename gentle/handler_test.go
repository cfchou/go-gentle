package gentle

import (
	"errors"
	"github.com/benbjohnson/clock"
	"github.com/cfchou/hystrix-go/hystrix"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRateLimitedHandler_Handle(t *testing.T) {
	// 1 msg/sec, no burst
	requestsInterval := 100 * time.Millisecond
	mhandler := &MockHandler{}
	handler := NewRateLimitedHandler(
		NewRateLimitedHandlerOpts("", "test",
			NewTokenBucketRateLimit(requestsInterval, 1)),
		mhandler)
	mm := &fakeMsg{id: "123"}
	mhandler.On("Handle", mm).Return(mm, nil)

	count := 3
	minimum := time.Duration(count-1) * requestsInterval
	var wg sync.WaitGroup
	wg.Add(count)
	begin := time.Now()
	for i := 0; i < count; i++ {
		go func() {
			_, err := handler.Handle(mm)
			assert.NoError(t, err)
			wg.Done()
		}()
	}
	wg.Wait()
	dura := time.Now().Sub(begin)
	log.Info("[Test] spent >= minmum?", "spent", dura, "minimum", minimum)
	assert.True(t, dura >= minimum)
}

func TestRetryHandler_Handle(t *testing.T) {
	// Test against mocked BackOff
	mfactory := &MockBackOffFactory{}
	mback := &MockBackOff{}
	// mock clock so that we don't need to wait for the real timer to move
	// forward
	mclock := clock.NewMock()
	opts := NewRetryHandlerOpts("", "test", mfactory)
	opts.Clock = mclock
	mhandler := &MockHandler{}
	handler := NewRetryHandler(opts, mhandler)

	// 1st: ok
	mm := &fakeMsg{id: "123"}
	call := mhandler.On("Handle", mm)
	call.Return(mm, nil)
	_, err := handler.Handle(mm)
	assert.NoError(t, err)

	// 2ed: err, trigger retry with backoffs
	fakeErr := errors.New("A fake error")
	call.Return((*fakeMsg)(nil), fakeErr)
	// create a backoff that fires 1 second for $count times
	count := 3
	timespanMinimum := time.Duration(count) * time.Second
	mfactory.On("NewBackOff").Return(mback)
	mback.On("Next").Return(func() time.Duration {
		if count == 0 {
			return BackOffStop
		}
		count--
		return 1 * time.Second
	})
	timespan := make(chan time.Duration, 1)
	go func() {
		begin := mclock.Now()
		_, err = handler.Handle(mm)
		// backoffs exhausted
		assert.EqualError(t, err, fakeErr.Error())
		timespan <- mclock.Now().Sub(begin)
	}()

	for {
		select {
		case dura := <-timespan:
			log.Info("[Test] spent >= minmum?", "spent", dura, "timespanMinimum", timespanMinimum)
			assert.True(t, dura >= timespanMinimum)
			return
		default:
			// advance an arbitrary time to pass all backoffs
			mclock.Add(1 * time.Second)
		}
	}
}

func TestRetryHandler_Get2(t *testing.T) {
	// Test against ConstantBackOff
	mclock := clock.NewMock()
	timespanMinimum := 16 * time.Second
	backOffOpts := NewConstantBackOffFactoryOpts(time.Second, timespanMinimum)
	backOffOpts.Clock = mclock
	backOffFactory := NewConstantBackOffFactory(backOffOpts)
	opts := NewRetryHandlerOpts("", "test", backOffFactory)
	opts.Clock = mclock
	mhandler := &MockHandler{}
	handler := NewRetryHandler(opts, mhandler)

	// 1st: ok
	mm := &fakeMsg{id: "123"}
	call := mhandler.On("Handle", mm)
	call.Return(mm, nil)
	_, err := handler.Handle(mm)
	assert.NoError(t, err)

	// 2ed: err, trigger retry with backoffs
	fakeErr := errors.New("A fake error")
	call.Return((*fakeMsg)(nil), fakeErr)

	timespan := make(chan time.Duration, 1)
	go func() {
		begin := mclock.Now()
		_, err = handler.Handle(mm)
		// backoffs exhausted
		assert.EqualError(t, err, fakeErr.Error())
		timespan <- mclock.Now().Sub(begin)
	}()

	for {
		select {
		case dura := <-timespan:
			log.Info("[Test] spent >= minmum?", "spent", dura, "timespanMinimum", timespanMinimum)
			assert.True(t, dura >= timespanMinimum)
			return
		default:
			// advance an arbitrary time to pass all backoffs
			mclock.Add(1 * time.Second)
		}
	}
}

func TestRetryHandler_Get3(t *testing.T) {
	// Test against ExponentialBackOff
	mclock := clock.NewMock()
	timespanMinimum := 1024 * time.Second
	backOffOpts := NewExponentialBackOffFactoryOpts(time.Second, 2.0, 256*time.Second, timespanMinimum)
	// No randomization to make the growth of backoff time approximately exponential.
	backOffOpts.RandomizationFactor = 0
	backOffOpts.Clock = mclock
	backOffFactory := NewExponentialBackOffFactory(backOffOpts)
	opts := NewRetryHandlerOpts("", "test", backOffFactory)
	opts.Clock = mclock
	mhandler := &MockHandler{}
	handler := NewRetryHandler(opts, mhandler)

	// 1st: ok
	mm := &fakeMsg{id: "123"}
	call := mhandler.On("Handle", mm)
	call.Return(mm, nil)
	_, err := handler.Handle(mm)
	assert.NoError(t, err)

	// 2ed: err, trigger retry with backoffs
	fakeErr := errors.New("A fake error")
	call.Return((*fakeMsg)(nil), fakeErr)

	timespan := make(chan time.Duration, 1)
	go func() {
		begin := mclock.Now()
		_, err = handler.Handle(mm)
		// backoffs exhausted
		assert.EqualError(t, err, fakeErr.Error())
		timespan <- mclock.Now().Sub(begin)
	}()

	for {
		select {
		case dura := <-timespan:
			log.Info("[Test] spent >= minmum?", "spent", dura, "timespanMinimum", timespanMinimum)
			assert.True(t, dura >= timespanMinimum)
			return
		default:
			// advance an arbitrary time to pass all backoffs
			mclock.Add(1 * time.Second)
		}
	}
}

func TestRetryHandler_Get4(t *testing.T) {
	// Test against concurrent retryHandler.Handle() and ConstantBackOff
	mclock := clock.NewMock()
	timespanMinimum := 16 * time.Second
	backOffOpts := NewConstantBackOffFactoryOpts(time.Second, timespanMinimum)
	backOffOpts.Clock = mclock
	backOffFactory := NewConstantBackOffFactory(backOffOpts)
	opts := NewRetryHandlerOpts("", "test", backOffFactory)
	opts.Clock = mclock
	mhandler := &MockHandler{}
	handler := NewRetryHandler(opts, mhandler)

	// 1st: ok
	mm := &fakeMsg{id: "123"}
	call := mhandler.On("Handle", mm)
	call.Return(mm, nil)
	_, err := handler.Handle(mm)
	assert.NoError(t, err)

	// 2ed: err, trigger retry with backoffs
	fakeErr := errors.New("A fake error")
	call.Return((*fakeMsg)(nil), fakeErr)

	count := 2
	wg := sync.WaitGroup{}
	wg.Add(count)
	done := make(chan struct{}, 1)

	for i := 0; i < count; i++ {
		go func() {
			begin := mclock.Now()
			_, err = handler.Handle(mm)
			// backoffs exhausted
			assert.EqualError(t, err, fakeErr.Error())
			dura := mclock.Now().Sub(begin)
			log.Info("[Test] spent >= minmum?", "spent", dura, "timespanMinimum", timespanMinimum)
			assert.True(t, dura >= timespanMinimum)
			wg.Done()
		}()

	}
	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	begin := mclock.Now()
	for {
		select {
		case <-done:
			log.Info("[Test] all done", "spent", mclock.Now().Sub(begin))
			return
		default:
			// advance an arbitrary time to pass all backoffs
			mclock.Add(1 * time.Second)
		}
	}
}

func TestRetryHandler_Get5(t *testing.T) {
	// Test against concurrent retryHandler.Handle() and ExponentialBackOff
	mclock := clock.NewMock()
	timespanMinimum := 1024 * time.Second
	backOffOpts := NewExponentialBackOffFactoryOpts(time.Second, 2.0, 256*time.Second, timespanMinimum)
	// No randomization to make the growth of backoff time approximately exponential.
	backOffOpts.RandomizationFactor = 0
	backOffOpts.Clock = mclock
	backOffFactory := NewExponentialBackOffFactory(backOffOpts)
	opts := NewRetryHandlerOpts("", "test", backOffFactory)
	opts.Clock = mclock
	mhandler := &MockHandler{}
	handler := NewRetryHandler(opts, mhandler)

	// 1st: ok
	mm := &fakeMsg{id: "123"}
	call := mhandler.On("Handle", mm)
	call.Return(mm, nil)
	_, err := handler.Handle(mm)
	assert.NoError(t, err)

	// 2ed: err, trigger retry with backoffs
	fakeErr := errors.New("A fake error")
	call.Return((*fakeMsg)(nil), fakeErr)

	count := 2
	wg := sync.WaitGroup{}
	wg.Add(count)
	done := make(chan struct{}, 1)

	for i := 0; i < count; i++ {
		go func() {
			begin := mclock.Now()
			_, err = handler.Handle(mm)
			// backoffs exhausted
			assert.EqualError(t, err, fakeErr.Error())
			dura := mclock.Now().Sub(begin)
			log.Info("[Test] spent >= minmum?", "spent", dura, "timespanMinimum", timespanMinimum)
			assert.True(t, dura >= timespanMinimum)
			wg.Done()
		}()

	}
	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	begin := mclock.Now()
	for {
		select {
		case <-done:
			log.Info("[Test] all done", "spent", mclock.Now().Sub(begin))
			return
		default:
			// advance an arbitrary time to pass all backoffs
			mclock.Add(1 * time.Second)
		}
	}
}

func TestBulkheadHandler_Handle(t *testing.T) {
	maxConcurrency := 4
	mhandler := &MockHandler{}

	handler := NewBulkheadHandler(
		NewBulkheadHandlerOpts("", "test", maxConcurrency),
		mhandler)
	mm := &fakeMsg{id: "123"}

	wg := &sync.WaitGroup{}
	wg.Add(maxConcurrency)
	block := make(chan struct{}, 0)
	mhandler.On("Handle", mm).Return(
		func(Message) Message {
			wg.Done()
			// every Handle() would be blocked here
			block <- struct{}{}
			return mm
		}, nil)

	for i := 0; i < maxConcurrency; i++ {
		go handler.Handle(mm)
	}

	// Wait() until $maxConcurrency of Handle() are blocked
	wg.Wait()
	// one more Handle() would cause ErrMaxConcurrency
	msg, err := handler.Handle(mm)
	assert.Equal(t, msg, nil)
	assert.EqualError(t, err, ErrMaxConcurrency.Error())
	// Release blocked
	for i := 0; i < maxConcurrency; i++ {
		<-block
	}
}

func TestCircuitBreakerHandler_Handle(t *testing.T) {
	defer hystrix.Flush()
	maxConcurrency := 4
	circuit := xid.New().String()
	mhandler := &MockHandler{}

	// requests exceeding MaxConcurrentRequests would get
	// ErrCbMaxConcurrency provided that Timeout is large enough for this
	// test case
	conf := NewDefaultCircuitBreakerConf()
	conf.MaxConcurrent = maxConcurrency
	conf.Timeout = 10 * time.Second
	conf.RegisterFor(circuit)

	handler := NewCircuitBreakerHandler(
		NewCircuitBreakerHandlerOpts("", "test", circuit),
		mhandler)
	mm := &fakeMsg{id: "123"}

	var wg sync.WaitGroup
	wg.Add(maxConcurrency)
	cond := sync.NewCond(&sync.Mutex{})
	mhandler.On("Handle", mm).Return(
		func(Message) Message {
			// wg.Done() here instead of in the loop guarantees Get() is
			// running by the circuit
			wg.Done()
			cond.L.Lock()
			cond.Wait()
			return mm
		}, nil)

	for i := 0; i < maxConcurrency; i++ {
		go func() {
			handler.Handle(mm)
		}()
	}
	// Make sure previous Handle() are all running before the next
	wg.Wait()
	// One more call while all previous calls sticking in the circuit
	_, err := handler.Handle(mm)
	assert.EqualError(t, err, ErrCbMaxConcurrency.Error())
	cond.Broadcast()
}

func TestCircuitBreakerHandler_Handle2(t *testing.T) {
	// Test ErrCbTimeout and subsequent ErrCbOpen
	defer hystrix.Flush()
	circuit := xid.New().String()
	mhandler := &MockHandler{}

	conf := NewDefaultCircuitBreakerConf()
	// Set RequestVolumeThreshold/ErrorPercentThreshold to be the most
	// sensitive. One request hits timeout would make the subsequent
	// requests coming within SleepWindow see ErrCbOpen
	conf.VolumeThreshold = 1
	conf.ErrorPercentThreshold = 1
	conf.SleepWindow = time.Second
	// A short Timeout to speed up the test
	conf.Timeout = time.Millisecond
	conf.RegisterFor(circuit)

	handler := NewCircuitBreakerHandler(
		NewCircuitBreakerHandlerOpts("", "test", circuit),
		mhandler)
	var nth int64
	newMsg := func() Message {
		tmp := atomic.AddInt64(&nth, 1)
		return &fakeMsg{id: strconv.FormatInt(tmp, 10)}
	}

	// Suspend longer than Timeout
	var toSuspend int64
	suspend := conf.Timeout + time.Millisecond
	mhandler.On("Handle", mock.AnythingOfType("*gentle.fakeMsg")).Return(
		func(mm Message) Message {
			if atomic.LoadInt64(&toSuspend) == 0 {
				time.Sleep(suspend)
			}
			return mm
		}, nil)

	// ErrCbTimeout then the subsequent requests eventually see ErrCbOpen.
	// "Eventually" because hystrix updates metrics asynchronously.
	for {
		log.Debug("[Test] try again for ErrCbOpen")
		_, err := handler.Handle(newMsg())
		if err == ErrCbOpen {
			break
		}
		// err could be nil if $suspend is short and scheduling is slow.
		// if that's the case, we'll try until threshold is reached.
	}

	// Disable toSuspend
	atomic.StoreInt64(&toSuspend, 1)
	time.Sleep(conf.SleepWindow)
	for {
		log.Debug("[Test] try again for no err")
		_, err := handler.Handle(newMsg())
		if err == nil {
			// In the end, circuit is closed because of no error.
			break
		}
		// err could be ErrCbOpen or even ErrCbTimeout if scheduling
		// is slow. If that's the case, we'll try until circuit is closed.
	}
}

func TestCircuitBreakerHandler_Handle3(t *testing.T) {
	// Test fakeErr and subsequent ErrCbOpen
	defer hystrix.Flush()
	circuit := xid.New().String()
	mhandler := &MockHandler{}

	conf := NewDefaultCircuitBreakerConf()
	// Set RequestVolumeThreshold/ErrorPercentThreshold to be the most
	// sensitive. One request returns error would make the subsequent
	// requests coming within SleepWindow see ErrCbOpen
	conf.VolumeThreshold = 1
	conf.ErrorPercentThreshold = 1
	conf.SleepWindow = time.Second
	conf.RegisterFor(circuit)

	handler := NewCircuitBreakerHandler(
		NewCircuitBreakerHandlerOpts("", "test", circuit),
		mhandler)
	mm := &fakeMsg{id: "123"}
	fakeErr := errors.New("A fake error")

	call := mhandler.On("Handle", mm)
	call.Return((*fakeMsg)(nil), fakeErr)

	// 1st call gets fakeErr
	_, err := handler.Handle(mm)
	assert.EqualError(t, err, fakeErr.Error())

	// Subsequent requests within SleepWindow eventually see ErrCbOpen.
	// "Eventually" because hystrix updates metrics asynchronously.
	for {
		log.Debug("[Test] try again for ErrCbOpen")
		_, err := handler.Handle(mm)
		if err == ErrCbOpen {
			break
		} else {
			assert.EqualError(t, err, fakeErr.Error())
		}
	}

	time.Sleep(conf.SleepWindow)
	call.Return(mm, nil)
	for {
		log.Debug("[Test] try again for no err")
		_, err := handler.Handle(mm)
		if err == nil {
			// In the end, circuit is closed because of no error.
			break
		} else {
			assert.EqualError(t, err, ErrCbOpen.Error())
		}
	}
}
