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
	"sync/atomic"
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
	var to_suspend int64
	suspend := conf.Timeout + time.Millisecond
	call := mhandler.On("Handle", mm)
	call.Run(func(args mock.Arguments) {
		if atomic.LoadInt64(&to_suspend) == 0 {
			time.Sleep(suspend)
		}
	})
	call.Return(mm, nil)

	// 1st call gets ErrCbTimeout
	_, err := handler.Handle(mm)
	assert.EqualError(t, err, ErrCbTimeout.Error())

	// Subsequent requests eventually see ErrCbOpen.
	// "Eventually" because hystrix updates metrics asynchronously.
	for {
		log.Debug("[Test] try again")
		_, err := handler.Handle(mm)
		if err == ErrCbOpen {
			break
		} else {
			assert.EqualError(t, err, ErrCbTimeout.Error())
		}
	}

	// Disable to_suspend
	atomic.StoreInt64(&to_suspend, 1)
	time.Sleep(conf.SleepWindow)
	for {
		_, err := handler.Handle(mm)
		if err == nil {
			// In the end, circuit is closed because of no error.
			break
		} else {
			assert.EqualError(t, err, ErrCbOpen.Error())
		}
	}
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
	for {
		log.Debug("[Test] try again")
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
		_, err := handler.Handle(mm)
		if err == nil {
			// In the end, circuit is closed because of no error.
			break
		} else {
			assert.EqualError(t, err, ErrCbOpen.Error())
		}
	}
}

