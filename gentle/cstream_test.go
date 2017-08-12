package gentle

import (
	"context"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestRateLimitedCStream_Get(t *testing.T) {
	// 1 msg/sec
	requestsInterval := 100 * time.Millisecond
	src, done := createInfiniteMessageChan()
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
	log.Info("[Test] spent >= minmum?", "spent", dura, "minimum", minimum)
	assert.True(t, dura >= minimum)
	done <- &struct{}{}
}