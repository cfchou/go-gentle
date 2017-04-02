package gentle

import (
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRateLimitedHandler_Handle(t *testing.T) {
	count := 3
	// 1 msg/sec, no burst
	requests_interval := 1000
	mhandler := &mockHandler{}
	handler := NewRateLimitedHandler("","test", mhandler,
		NewTokenBucketRateLimit(requests_interval, 1))

	mm := &mockMsg{}
	mm.On("Id").Return("123")
	mhandler.On("Handle", mm).Return(mm, nil)

	minimum := time.Duration((count-1)*requests_interval) * time.Millisecond
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
	handler := NewRetryHandler("","test", mhandler, backoffs)

	mm := &mockMsg{}

	// 1st: ok
	mm.On("Id").Return("123")
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
	count := 8
	max_concurrency := 4
	mhandler := &mockHandler{}
	handler := NewBulkheadHandler("","test", mhandler, max_concurrency)
	mm := &mockMsg{}
	mm.On("Id").Return("123")

	suspend := 100 * time.Millisecond
	lock := &sync.RWMutex{}
	calling := 0
	callings := []int{}
	call := mhandler.On("Handle", mm)
	call.Run(func(args mock.Arguments) {
		// add calling
		lock.Lock()
		calling++
		callings = append(callings, calling)
		lock.Unlock()
		time.Sleep(suspend)
		// release calling
		lock.Lock()
		calling--
		lock.Unlock()
	})
	call.Return(mm, nil)

	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			msg, err := handler.Handle(mm)
			wg.Done()
			assert.NoError(t, err)
			assert.Equal(t, msg.Id(), mm.Id())
		}()
	}
	wg.Wait()
	assert.True(t, count == len(callings))
	for _, c := range callings {
		assert.True(t, c <= max_concurrency)
	}
}

func TestCircuitBreakerHandler_Handle(t *testing.T) {
	max_concurrency := 4
	circuit := xid.New().String()
	mhandler := &mockHandler{}

	// requests exceeding MaxConcurrentRequests would get ErrMaxConcurrency
	// provided Timeout is large enough for this test case
	conf := GetHystrixDefaultConfig()
	conf.MaxConcurrentRequests = max_concurrency
	conf.Timeout = 10000
	hystrix.ConfigureCommand(circuit, *conf)

	handler := NewCircuitBreakerHandler("","test", mhandler, circuit)
	mm := &mockMsg{}
	mm.On("Id").Return("123")

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
	assert.EqualError(t, err, ErrMaxConcurrency.Error())
	cond.Broadcast()
}

func TestCircuitBreakerHandler_Handle2(t *testing.T) {
	// Test ErrTimeout and subsequent ErrCircuitOpen
	circuit := xid.New().String()
	mhandler := &mockHandler{}
	count := 3

	conf := GetHystrixDefaultConfig()
	// Set RequestVolumeThreshold/ErrorPercentThreshold to be the most
	// sensitive. One request hits timeout would make the subsequent
	// requests coming within SleepWindow see ErrCircuitOpen
	conf.RequestVolumeThreshold = 1
	conf.ErrorPercentThreshold = 1
	conf.SleepWindow = 1000
	conf.Timeout = 1000
	hystrix.ConfigureCommand(circuit, *conf)

	handler := NewCircuitBreakerHandler("","test", mhandler, circuit)

	mm := &mockMsg{}
	mm.On("Id").Return("123")

	// Suspend longer than Timeout
	suspend := time.Duration(conf.Timeout+500) * time.Millisecond
	call := mhandler.On("Handle", mm)
	call.Run(func(args mock.Arguments) {
		time.Sleep(suspend)
	})
	call.Return(mm, nil)

	// 1st call gets ErrTimeout which make subsequent requests within
	// SleepWindow see ErrCircuitOpen
	_, err := handler.Handle(mm)
	assert.EqualError(t, err, ErrTimeout.Error())

	var called int64
	call.Run(func(args mock.Arguments) {
		atomic.StoreInt64(&called, 1)
	})
	for i := 0; i < count; i++ {
		_, err := handler.Handle(mm)
		assert.EqualError(t, err, hystrix.ErrCircuitOpen.Error())
	}
	// ErrCircuitOpen prevents Handle() from execution.
	assert.Equal(t, atomic.LoadInt64(&called), int64(0))

	// After SleepWindow, circuit becomes half-open. Only one successful
	// case is needed to close the circuit.
	time.Sleep(IntToMillis(conf.SleepWindow))
	call.Run(func(args mock.Arguments) { /* no-op */ })
	_, err = handler.Handle(mm)
	assert.NoError(t, err)
	// In the end, circuit is closed because of no error.
}

func TestCircuitBreakerHandler_Handle3(t *testing.T) {
	// Test mockErr and subsequent ErrCircuitOpen
	circuit := xid.New().String()
	mhandler := &mockHandler{}
	count := 3

	conf := GetHystrixDefaultConfig()
	// Set RequestVolumeThreshold/ErrorPercentThreshold to be the most
	// sensitive. One request returns error would make the subsequent
	// requests coming within SleepWindow see ErrCircuitOpen
	conf.RequestVolumeThreshold = 1
	conf.ErrorPercentThreshold = 1
	conf.SleepWindow = 1000
	hystrix.ConfigureCommand(circuit, *conf)

	handler := NewCircuitBreakerHandler("","test", mhandler, circuit)

	mm := &mockMsg{}
	mm.On("Id").Return("123")

	mockErr := errors.New("A mocked error")

	call := mhandler.On("Handle", mm)
	call.Return(nil, mockErr)

	// 1st call gets mockErr which make subsequent requests within
	// SleepWindow see ErrCircuitOpen
	_, err := handler.Handle(mm)
	assert.EqualError(t, err, mockErr.Error())

	var called int64
	call.Run(func(args mock.Arguments) {
		atomic.StoreInt64(&called, 1)
	})
	for i := 0; i < count; i++ {
		_, err := handler.Handle(mm)
		assert.EqualError(t, err, hystrix.ErrCircuitOpen.Error())
	}

	// ErrCircuitOpen prevents Handle() from execution.
	assert.Equal(t, atomic.LoadInt64(&called), int64(0))

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
	count := 3

	conf := GetHystrixDefaultConfig()
	// Set ErrorPercentThreshold to be the most sensitive(1%). Once
	// RequestVolumeThreshold exceeded, the circuit becomes open.
	conf.RequestVolumeThreshold = count + 3
	conf.ErrorPercentThreshold = 1
	conf.SleepWindow = 1000
	hystrix.ConfigureCommand(circuit, *conf)

	handler := NewCircuitBreakerHandler("", "test", mhandler, circuit)
	mockErr := errors.New("A mocked error")

	mm := &mockMsg{}
	mm.On("Id").Return("123")
	call := mhandler.On("Handle", mm)

	// count is strictly smaller than RequestVolumeThreshold. So circuit is
	// still closed.
	for i := 0; i < count; i++ {
		call.Return(nil, mockErr)
		_, err := handler.Handle(mm)
		assert.EqualError(t, err, mockErr.Error())
	}

	// A success on the closed Circuit.
	call.Return(mm, nil)
	_, err := handler.Handle(mm)
	assert.NoError(t, err)

	// Until now, we have $count failed cases and 1 successful case. Need
	// $(RequestVolumeThreshold - count - 1) more cases(doesn't matter
	// successful or failed because ErrorPercentThreshold is the most
	// sensitive) to pass RequestVolumeThreshold to make the circuit open.
	for i := 0; i < conf.RequestVolumeThreshold; i++ {
		call.Return(mm, nil)
		_, err := handler.Handle(mm)
		if i < conf.RequestVolumeThreshold-count-1 {
			assert.NoError(t, err)
		} else {
			assert.EqualError(t, err, hystrix.ErrCircuitOpen.Error())
		}
	}
}
