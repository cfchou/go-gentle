package service

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
	handler := NewRateLimitedHandler("test", mhandler,
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
	handler := NewRetryHandler("test", mhandler, func() []time.Duration {
		return backoffs
	})

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
	handler := NewBulkheadHandler("test", mhandler, max_concurrency)

	suspend := 1 * time.Second
	maximum := suspend*time.Duration((count+max_concurrency-1)/max_concurrency) + time.Second

	mm := &mockMsg{}
	mm.On("Id").Return("123")
	calling := 0
	call := mhandler.On("Handle", mm)
	call.Run(func(args mock.Arguments) {
		calling++
		time.Sleep(suspend)
	})
	call.Return(mm, nil)

	var wg sync.WaitGroup
	wg.Add(count)
	begin := time.Now()
	for i := 0; i < count; i++ {
		go func() {
			msg, err := handler.Handle(mm)
			wg.Done()
			assert.NoError(t, err)
			assert.Equal(t, msg.Id(), mm.Id())
		}()
	}
	wg.Wait()
	dura := time.Now().Sub(begin)
	log.Info("[Test] spent <= maximum?", "spent", dura, "maximum", maximum)
	assert.True(t, dura <= maximum)
}

func TestCircuitBreakerHandler_Handle(t *testing.T) {
	count := 8
	max_concurrency := 4
	circuit := xid.New().String()
	mhandler := &mockHandler{}

	// requests exceeding MaxConcurrentRequests would get ErrMaxConcurrency
	conf := GetHystrixDefaultConfig()
	conf.MaxConcurrentRequests = max_concurrency
	hystrix.ConfigureCommand(circuit, *conf)

	handler := NewCircuitBreakerHandler("test", mhandler, circuit)

	suspend := 1 * time.Second
	mm := &mockMsg{}
	mm.On("Id").Return("123")
	var calling int32
	call := mhandler.On("Handle", mm)
	call.Run(func(args mock.Arguments) {
		atomic.AddInt32(&calling, 1)
		time.Sleep(suspend)
	})
	call.Return(mm, nil)

	lock := &sync.Mutex{}
	var all_errors []*error
	//
	tm := time.NewTimer(suspend + time.Second)
	for i := 0; i < count; i++ {
		go func() {
			_, err := handler.Handle(mm)
			if err != nil {
				lock.Lock()
				defer lock.Unlock()
				all_errors = append(all_errors, &err)
			}
		}()
	}
	<-tm.C
	// ErrMaxConcurrency prevents Handle from execution.
	assert.Equal(t, atomic.LoadInt32(&calling), int32(max_concurrency))
	lock.Lock()
	defer lock.Unlock()
	for _, e := range all_errors {
		assert.EqualError(t, *e, hystrix.ErrMaxConcurrency.Error())
		log.Info("[Test]", "err", *e)
	}
	assert.Equal(t, count-max_concurrency, len(all_errors))
}

func TestCircuitBreakerHandler_Handle2(t *testing.T) {
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

	handler := NewCircuitBreakerHandler("test", mhandler, circuit)

	mm := &mockMsg{}
	mm.On("Id").Return("123")

	suspend := time.Duration(conf.Timeout+500) * time.Millisecond
	var calling int32
	call := mhandler.On("Handle", mm)
	call.Run(func(args mock.Arguments) {
		atomic.AddInt32(&calling, 1)
		time.Sleep(suspend)
	})
	call.Return(mm, nil)

	// 1st takes more than timeout. Though no error, it causes
	// ErrCircuitOpen for the subsequent requests.
	begin := time.Now()
	_, err := handler.Handle(mm)
	assert.NoError(t, err)
	dura := time.Now().Sub(begin)
	assert.True(t, dura > suspend)

	for i := 0; i < count; i++ {
		_, err := handler.Handle(mm)
		assert.EqualError(t, err, hystrix.ErrCircuitOpen.Error())
	}
	// ErrCircuitOpen prevents Handle from execution.
	assert.Equal(t, atomic.LoadInt32(&calling), int32(1))

	// After SleepWindow, circuit becomes half-open. Only one successful
	// case is needed to close the circuit.
	time.Sleep(IntToMillis(conf.SleepWindow))
	call.Run(func(args mock.Arguments) { /* no-op */ })
	call.Return(mm, nil)
	_, err = handler.Handle(mm)
	assert.NoError(t, err)
	// In the end, circuit is closed because of no error.
}

func TestCircuitBreakerHandler_Handle3(t *testing.T) {
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

	handler := NewCircuitBreakerHandler("test", mhandler, circuit)

	mm := &mockMsg{}
	mm.On("Id").Return("123")

	mockErr := errors.New("A mocked error")

	var calling int32
	call := mhandler.On("Handle", mm)
	call.Run(func(args mock.Arguments) {
		atomic.AddInt32(&calling, 1)
	})
	call.Return(nil, mockErr)

	// 1st return mockErr, it causes ErrCircuitOpen for the subsequent
	// requests.
	_, err := handler.Handle(mm)
	assert.EqualError(t, err, mockErr.Error())

	for i := 0; i < count; i++ {
		_, err := handler.Handle(mm)
		assert.EqualError(t, err, hystrix.ErrCircuitOpen.Error())
	}
	// ErrCircuitOpen prevents Handle from execution.
	assert.Equal(t, atomic.LoadInt32(&calling), int32(1))

	// After SleepWindow, circuit becomes half-open. Only one successful
	// case is needed to close the circuit.
	time.Sleep(IntToMillis(conf.SleepWindow))
	call.Run(func(args mock.Arguments) { /* no-op */ })
	call.Return(mm, nil)
	_, err = handler.Handle(mm)
	assert.NoError(t, err)
}

func TestCircuitBreakerHandler_Handle4(t *testing.T) {
	circuit := xid.New().String()
	mhandler := &mockHandler{}
	count := 3

	conf := GetHystrixDefaultConfig()
	// Set RequestVolumeThreshold/ErrorPercentThreshold to be the most
	// sensitive. One request returns error would make the subsequent
	// requests coming within SleepWindow see ErrCircuitOpen
	conf.RequestVolumeThreshold = count + 3
	conf.ErrorPercentThreshold = 1
	conf.SleepWindow = 1000
	hystrix.ConfigureCommand(circuit, *conf)

	handler := NewCircuitBreakerHandler("test", mhandler, circuit)
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

	call.Return(mm, nil)
	_, err := handler.Handle(mm)
	assert.NoError(t, err)

	// Until now, we have $count failed cases and 1 successful case. Need
	// $(RequestVolumeThreshold - count - 1) cases, doesn't matter successful
	// or failed because ErrorPercentThreshold is extremely low, to make the
	// circuit open.
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
