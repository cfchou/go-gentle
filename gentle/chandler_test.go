package gentle

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"sync"
	"testing"
	"time"
)

func TestRateLimitedCHandler_Handle(t *testing.T) {
	requestsInterval := 100 * time.Millisecond
	mhandler := &MockCHandler{}
	handler := NewRateLimitedCHandler(
		NewRateLimitedCHandlerOpts("", "test",
			NewTokenBucketRateLimit(requestsInterval, 1)),
		mhandler)
	mm := &fakeMsg{id: "123"}
	mhandler.On("Handle", mock.Anything, mm).Return(mm, nil)
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
