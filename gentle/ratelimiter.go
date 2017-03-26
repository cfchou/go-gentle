// A wrapper for "github.com/juju/ratelimit".

package gentle

import (
	"errors"
	"github.com/juju/ratelimit"
	"time"
)

type TokenBucketRateLimit struct {
	bucket *ratelimit.Bucket
}

// $requests_interval The minimum interval, in milliseconds, between two
// consecutive requests.
// N is the amount of requests allowed when a burst of requests coming
// in after not seeing requests for N * RequestsInterval. N is capped by
// $max_requests_burst.
// If $max_requests_burst == 1, then no burst allowed.
func NewTokenBucketRateLimit(requests_interval int, max_requests_burst int) *TokenBucketRateLimit {
	if max_requests_burst <= 0 {
		panic(errors.New("max_request_burst must be greater than 0"))
	}
	return &TokenBucketRateLimit{
		bucket: ratelimit.NewBucket(
			IntToMillis(requests_interval),
			int64(max_requests_burst)),
	}
}

func (rl *TokenBucketRateLimit) Wait(count int, timeout time.Duration) bool {
	if count <= 0 {
		panic(errors.New("count must be greater than 0"))
	}
	if timeout == 0 {
		rl.bucket.Wait(int64(count))
		return true
	}
	return rl.bucket.WaitMaxDuration(int64(count), timeout)
}
