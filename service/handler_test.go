// vim:fileencoding=utf-8
package service

import (
	"testing"
	"time"
	"github.com/stretchr/testify/assert"
	"sync"
	"errors"
	"github.com/stretchr/testify/mock"
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
	maximum := suspend * time.Duration((count + max_concurrency - 1) / max_concurrency) + time.Second

	mm := &mockMsg{}
	mm.On("Id").Return("123")
	calling := 0
	call := mhandler.On("Handle", mm)
	call.Run(func (args mock.Arguments) {
		calling++
		time.Sleep(suspend)
	})
	call.Return(mm, nil)

	var wg sync.WaitGroup
	wg.Add(count)
	begin := time.Now()
	for i :=0; i < count; i++ {
		go func () {
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
