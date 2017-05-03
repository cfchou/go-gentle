package gentle

import (
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"sync"
	"testing"
	"time"
)

func TestRateLimitedHandler_Handle(t *testing.T) {
	// 1 msg/sec, no burst
	requests_interval := 100 * time.Millisecond
	mhandler := &mockHandler{}
	handler := NewRateLimitedHandler("", "test", mhandler,
		NewTokenBucketRateLimit(requests_interval, 1))
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
	backoffs := []time.Duration{1 * time.Second, 2 * time.Second}
	minimum := func(backoffs []time.Duration) time.Duration {
		dura_sum := 0 * time.Second
		for _, dura := range backoffs {
			dura_sum += dura
		}
		return dura_sum
	}(backoffs)

	mhandler := &mockHandler{}
	handler := NewRetryHandler("", "test", mhandler, backoffs)
	mm := &fakeMsg{id: "123"}

	// 1st: ok
	call := mhandler.On("Handle", mm)
	call.Return(mm, nil)

	_, err := handler.Handle(mm)
	assert.NoError(t, err)

	// 2ed: err, trigger retry with backoffs
	mockErr := errors.New("A mocked error")
	call.Return(nil, mockErr)

	begin := time.Now()
	_, err = handler.Handle(mm)
	dura := time.Now().Sub(begin)
	// backoffs exhausted
	assert.EqualError(t, err, mockErr.Error())
	log.Info("[Test] spent >= minmum?", "spent", dura, "minimum", minimum)
	assert.True(t, dura >= minimum)
}

func TestBulkheadHandler_Handle(t *testing.T) {
	max_concurrency := 4
	mhandler := &mockHandler{}
	handler := NewBulkheadHandler("", "test", mhandler, max_concurrency)
	mm := &fakeMsg{id: "123"}

	wg := &sync.WaitGroup{}
	wg.Add(max_concurrency)
	block := make(chan struct{}, 0)
	call := mhandler.On("Handle", mm)
	call.Run(func(args mock.Arguments) {
		wg.Done()
		block <- struct{}{}
	})
	call.Return(mm, nil)

	for i := 0; i < max_concurrency; i++ {
		go handler.Handle(mm)
	}

	wg.Wait()
	msg, err := handler.Handle(mm)
	assert.Equal(t, msg, nil)
	assert.EqualError(t, err, ErrMaxConcurrency.Error())
	for i := 0; i < max_concurrency; i++ {
		<-block
	}
}

func TestCircuitBreakerHandler_Handle(t *testing.T) {
	max_concurrency := 4
	circuit := xid.New().String()
	mhandler := &mockHandler{}

	// requests exceeding MaxConcurrentRequests would get ErrCbMaxConcurrency
	// provided Timeout is large enough for this test case
	conf := GetHystrixDefaultConfig()
	conf.MaxConcurrentRequests = max_concurrency
	conf.Timeout = 10000
	hystrix.ConfigureCommand(circuit, *conf)

	handler := NewCircuitBreakerHandler("", "test", mhandler, circuit)
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

	conf := GetHystrixDefaultConfig()
	// Set RequestVolumeThreshold/ErrorPercentThreshold to be the most
	// sensitive. One request hits timeout would make the subsequent
	// requests coming within SleepWindow see ErrCbOpen
	conf.RequestVolumeThreshold = 1
	conf.ErrorPercentThreshold = 1
	conf.SleepWindow = 1000
	conf.Timeout = 1000
	hystrix.ConfigureCommand(circuit, *conf)

	handler := NewCircuitBreakerHandler("", "test", mhandler, circuit)
	mm := &fakeMsg{id: "123"}

	// Suspend longer than Timeout
	cond := sync.NewCond(&sync.Mutex{})
	suspended := false
	suspend := time.Duration(conf.Timeout+1000) * time.Millisecond
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
	// "Eventually" because hystrix error metrics asynchronously.
	tm := time.NewTimer(IntToMillis(conf.SleepWindow))
LOOP:
	for {
		log.Debug("[Test] try again")
		select {
		case <-tm.C:
			assert.Fail(t, "[Test] SleepWindow not long enough")
		default:
			time.Sleep(100 * time.Millisecond)
			_, err := handler.Handle(mm)
			if err == ErrCbOpen {
				tm.Stop()
				break LOOP
			}
		}
	}

	// Wait to prevent data race. At this moment 1st call might be still
	// running.
	cond.L.Lock()
	for !suspended {
		cond.Wait()
	}
	cond.L.Unlock()

	// After SleepWindow, circuit becomes half-open. Only one successful
	// case is needed to close the circuit.
	time.Sleep(IntToMillis(conf.SleepWindow))
	_, err = handler.Handle(mm)
	assert.NoError(t, err)
	// In the end, circuit is closed because of no error.
}

func TestCircuitBreakerHandler_Handle3(t *testing.T) {
	// Test mockErr and subsequent ErrCbOpen
	circuit := xid.New().String()
	mhandler := &mockHandler{}

	conf := GetHystrixDefaultConfig()
	// Set RequestVolumeThreshold/ErrorPercentThreshold to be the most
	// sensitive. One request returns error would make the subsequent
	// requests coming within SleepWindow see ErrCbOpen
	conf.RequestVolumeThreshold = 1
	conf.ErrorPercentThreshold = 1
	conf.SleepWindow = 1000
	hystrix.ConfigureCommand(circuit, *conf)

	handler := NewCircuitBreakerHandler("", "test", mhandler, circuit)
	mm := &fakeMsg{id: "123"}
	fakeErr := errors.New("A fake error")

	call := mhandler.On("Handle", mm)
	call.Return(nil, fakeErr)

	// 1st call gets fakeErr
	_, err := handler.Handle(mm)
	assert.EqualError(t, err, fakeErr.Error())

	// Subsequent requests within SleepWindow eventually see ErrCbOpen.
	// "Eventually" because hystrix error metrics asynchronously.
	tm := time.NewTimer(IntToMillis(conf.SleepWindow))
LOOP:
	for {
		log.Debug("[Test] try again")
		select {
		case <-tm.C:
			assert.Fail(t, "[Test] SleepWindow not long enough")
		default:
			time.Sleep(100 * time.Millisecond)
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
	time.Sleep(IntToMillis(conf.SleepWindow))
	call.Run(func(args mock.Arguments) { /* no-op */ })
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

	conf := GetHystrixDefaultConfig()
	// Set ErrorPercentThreshold to be the most sensitive(1%). Once
	// RequestVolumeThreshold exceeded, the circuit becomes open.
	conf.RequestVolumeThreshold = countErr + countSucc + countRest
	conf.ErrorPercentThreshold = 1
	conf.SleepWindow = 10000
	hystrix.ConfigureCommand(circuit, *conf)

	handler := NewCircuitBreakerHandler("", "test", mhandler, circuit)
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

	// A success on the closed Circuit.
	call.Return(mm, nil)
	for i := 0; i < countSucc; i++ {
		// A success on the closed Circuit.
		_, err := handler.Handle(mm)
		assert.NoError(t, err)
	}

	// Subsequent requests within SleepWindow eventually see ErrCbOpen.
	// "Eventually" because hystrix error metrics asynchronously.
	tm := time.NewTimer(IntToMillis(conf.SleepWindow))
LOOP:
	for {
		log.Debug("[Test] try again")
		select {
		case <-tm.C:
			assert.Fail(t, "[Test] SleepWindow not long enough")
		default:
			time.Sleep(100 * time.Millisecond)
			_, err := handler.Handle(mm)
			if err == ErrCbOpen {
				tm.Stop()
				break LOOP
			}
		}
	}
}
