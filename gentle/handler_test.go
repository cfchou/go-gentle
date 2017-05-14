package gentle

import (
	"errors"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"sync"
	"testing"
	"time"
	"github.com/benbjohnson/clock"
)

func TestRateLimitedHandler_Handle(t *testing.T) {
	// 1 msg/sec, no burst
	requests_interval := 100 * time.Millisecond
	mhandler := &mockHandler{}
	handler := NewRateLimitedHandler(
		*NewRateLimitedHandlerOpts("", "test",
			NewTokenBucketRateLimit(requests_interval, 1)),
		mhandler)
	mm := &fakeMsg{id: "123"}
	mhandler.On("Handle", mm).Return(mm, nil)

	count := 3
	minimum := time.Duration(count-1) * requests_interval
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
	mback := &mockBackOff{}
	// mock clock so that we don't need to wait for the real timer to move
	// forward
	mclock := clock.NewMock()
	opts := NewRetryHandlerOpts("", "test", mback)
	opts.Clock = mclock
	mhandler := &mockHandler{}
	handler := NewRetryHandler(*opts, mhandler)

	// 1st: ok
	mm := &fakeMsg{id: "123"}
	call := mhandler.On("Handle", mm)
	call.Return(mm, nil)
	_, err := handler.Handle(mm)
	assert.NoError(t, err)

	// 2ed: err, trigger retry with backoffs
	fakeErr := errors.New("A mocked error")
	call.Return(nil, fakeErr)
	count := 3
	timespan_minimum := time.Duration(count) * time.Second
	mback_next := mback.On("Next")
	mback_next.Run(func(args mock.Arguments) {
		if count == 0 {
			mback_next.Return(BackOffStop)
		} else {
			count--
			mback_next.Return(1 * time.Second)
		}
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
		case dura :=<-timespan:
			log.Info("[Test] spent >= minmum?", "spent", dura, "timespan_minimum", timespan_minimum)
			assert.True(t, dura >= timespan_minimum)
			return
		default:
			// advance an arbitrary time to pass all backoffs
			mclock.Add(1*time.Second)
		}
	}
}

func TestBulkheadHandler_Handle(t *testing.T) {
	max_concurrency := 4
	mhandler := &mockHandler{}

	handler := NewBulkheadHandler(
		*NewBulkheadHandlerOpts("", "test", max_concurrency),
		mhandler)
	mm := &fakeMsg{id: "123"}

	wg := &sync.WaitGroup{}
	wg.Add(max_concurrency)
	block := make(chan struct{}, 0)
	call := mhandler.On("Handle", mm)
	call.Run(func(args mock.Arguments) {
		wg.Done()
		// every Handle() would be blocked here
		block <- struct{}{}
	})
	call.Return(mm, nil)

	for i := 0; i < max_concurrency; i++ {
		go handler.Handle(mm)
	}

	// Wait() until $max_concurrency of Handle() are blocked
	wg.Wait()
	// one more Handle() would cause ErrMaxConcurrency
	msg, err := handler.Handle(mm)
	assert.Equal(t, msg, nil)
	assert.EqualError(t, err, ErrMaxConcurrency.Error())
	// Release blocked
	for i := 0; i < max_concurrency; i++ {
		<-block
	}
}

func TestCircuitBreakerHandler_Handle(t *testing.T) {
	max_concurrency := 4
	circuit := xid.New().String()
	mhandler := &mockHandler{}

	// requests exceeding MaxConcurrentRequests would get
	// ErrCbMaxConcurrency provided that Timeout is large enough for this
	// test case
	conf := NewDefaultCircuitBreakerConf()
	conf.MaxConcurrent = max_concurrency
	conf.Timeout = 10 * time.Second
	conf.RegisterFor(circuit)

	handler := NewCircuitBreakerHandler(
		*NewCircuitBreakerHandlerOpts("", "test", circuit),
		mhandler)
	mm := &fakeMsg{id: "123"}

	var wg sync.WaitGroup
	wg.Add(max_concurrency)
	cond := sync.NewCond(&sync.Mutex{})
	call := mhandler.On("Handle", mm)
	call.Run(func(args mock.Arguments) {
		// wg.Done() here instead of in the loop guarantees Get() is
		// running by the circuit
		wg.Done()
		cond.L.Lock()
		cond.Wait()
	})
	call.Return(mm, nil)

	for i := 0; i < max_concurrency; i++ {
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
	circuit := xid.New().String()
	mhandler := &mockHandler{}

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
		*NewCircuitBreakerHandlerOpts("", "test", circuit),
		mhandler)
	mm := &fakeMsg{id: "123"}

	// Suspend longer than Timeout
	cond := sync.NewCond(&sync.Mutex{})
	suspended := false
	suspend := conf.Timeout + time.Millisecond
	call := mhandler.On("Handle", mm)
	call.Run(func(args mock.Arguments) {
		cond.L.Lock()
		defer cond.L.Unlock()
		if !suspended {
			// 1st call would suspend for tripping ErrCbTimeout
			suspended = true
			time.Sleep(suspend)
			cond.Broadcast()
		}
	})
	call.Return(mm, nil)

	// 1st call gets ErrCbTimeout
	_, err := handler.Handle(mm)
	assert.EqualError(t, err, ErrCbTimeout.Error())

	// Subsequent requests within SleepWindow eventually see ErrCbOpen.
	// "Eventually" because hystrix updates metrics asynchronously.
	tm := time.NewTimer(conf.SleepWindow)
LOOP:
	for {
		log.Debug("[Test] try again")
		select {
		case <-tm.C:
			assert.Fail(t, "[Test] SleepWindow not long enough")
		default:
			// call Handle() many times until circuit becomes open
			_, err := handler.Handle(mm)
			if err == ErrCbOpen {
				tm.Stop()
				break LOOP
			}
		}
	}

	// Wait to prevent data race because 1st call might still be running.
	cond.L.Lock()
	for !suspended {
		cond.Wait()
	}
	cond.L.Unlock()

	// After SleepWindow, circuit becomes half-open. Because
	// ErrorPercentThreshold is extremely low so only one successful case
	// is needed to close the circuit.
	time.Sleep(conf.SleepWindow)
	_, err = handler.Handle(mm)
	assert.NoError(t, err)
	// In the end, circuit is closed because of no error.
}

func TestCircuitBreakerHandler_Handle3(t *testing.T) {
	// Test fakeErr and subsequent ErrCbOpen
	circuit := xid.New().String()
	mhandler := &mockHandler{}

	conf := NewDefaultCircuitBreakerConf()
	// Set RequestVolumeThreshold/ErrorPercentThreshold to be the most
	// sensitive. One request returns error would make the subsequent
	// requests coming within SleepWindow see ErrCbOpen
	conf.VolumeThreshold = 1
	conf.ErrorPercentThreshold = 1
	conf.SleepWindow = time.Second
	conf.RegisterFor(circuit)

	handler := NewCircuitBreakerHandler(
		*NewCircuitBreakerHandlerOpts("", "test", circuit),
		mhandler)
	mm := &fakeMsg{id: "123"}
	fakeErr := errors.New("A fake error")

	call := mhandler.On("Handle", mm)
	call.Return(nil, fakeErr)

	// 1st call gets fakeErr
	_, err := handler.Handle(mm)
	assert.EqualError(t, err, fakeErr.Error())

	// Subsequent requests within SleepWindow eventually see ErrCbOpen.
	// "Eventually" because hystrix updates metrics asynchronously.
	tm := time.NewTimer(conf.SleepWindow)
LOOP:
	for {
		log.Debug("[Test] try again")
		select {
		case <-tm.C:
			assert.Fail(t, "[Test] SleepWindow not long enough")
		default:
			// call Handle() many times until circuit becomes open
			_, err := handler.Handle(mm)
			if err == ErrCbOpen {
				tm.Stop()
				break LOOP
			}
		}
	}

	// Once circuit is opened, subsequent calls should not run.
	call.Run(func(args mock.Arguments) {
		assert.Fail(t, "[Test] Should not run")
	})
	handler.Handle(mm)

	// After SleepWindow, circuit becomes half-open. Only one successful
	// case is needed to close the circuit.
	time.Sleep(conf.SleepWindow)
	call.Run(func(args mock.Arguments) {
		// no-op, for replacing previous one.
	})
	call.Return(mm, nil)
	_, err = handler.Handle(mm)
	assert.NoError(t, err)
}

func TestCircuitBreakerHandler_Handle4(t *testing.T) {
	// Test RequestVolumeThreshold/ErrorPercentThreshold
	circuit := xid.New().String()
	mhandler := &mockHandler{}
	countErr := 3
	countSucc := 1
	countRest := 3

	conf := NewDefaultCircuitBreakerConf()
	// Set ErrorPercentThreshold to be the most sensitive(1%). Once
	// RequestVolumeThreshold exceeded, the circuit becomes open.
	conf.VolumeThreshold = countErr + countSucc + countRest
	conf.ErrorPercentThreshold = 1
	conf.SleepWindow = time.Second
	conf.RegisterFor(circuit)

	handler := NewCircuitBreakerHandler(
		*NewCircuitBreakerHandlerOpts("", "test", circuit),
		mhandler)
	fakeErr := errors.New("A fake error")
	mm := &fakeMsg{id: "123"}
	call := mhandler.On("Handle", mm)

	// countErr is strictly smaller than RequestVolumeThreshold. So circuit
	// is still closed.
	call.Return(nil, fakeErr)
	for i := 0; i < countErr; i++ {
		_, err := handler.Handle(mm)
		assert.EqualError(t, err, fakeErr.Error())
	}

	call.Return(mm, nil)
	for i := 0; i < countSucc; i++ {
		// A success on the closed Circuit.
		_, err := handler.Handle(mm)
		assert.NoError(t, err)
	}

	// Subsequent requests within SleepWindow eventually see ErrCbOpen.
	// "Eventually" because hystrix updates metrics asynchronously.
	tm := time.NewTimer(conf.SleepWindow)
LOOP:
	for {
		log.Debug("[Test] try again")
		select {
		case <-tm.C:
			assert.Fail(t, "[Test] SleepWindow not long enough")
		default:
			_, err := handler.Handle(mm)
			if err == ErrCbOpen {
				tm.Stop()
				break LOOP
			}
		}
	}
}
