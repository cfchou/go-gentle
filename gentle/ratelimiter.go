// A wrapper for "github.com/juju/ratelimit".

package gentle

import (
	"errors"
	"github.com/juju/ratelimit"
	"time"
)

// RateLimit is an interface for a "token bucket" algorithm.
type RateLimit interface {
	// Wait for $count tokens to be granted(return true) or timeout(return
	// false). If $timeout == 0, it would block as long as it needs.
	Wait(count int, timeout time.Duration) bool
}

type TokenBucketRateLimit struct {
	bucket *ratelimit.Bucket
}

// $requestsInterval The minimum interval, in milliseconds, between two
// consecutive requests.
// N is the amount of requests allowed when a burst of requests coming
// in after not seeing requests for N * RequestsInterval. N is capped by
// $maxRequestBurst.
// If $maxRequestBurst == 1, then no burst allowed.
func NewTokenBucketRateLimit(requestsInterval time.Duration,
	maxRequestBurst int) *TokenBucketRateLimit {

	if maxRequestBurst <= 0 {
		panic(errors.New("max_request_burst must be greater than 0"))
	}
	return &TokenBucketRateLimit{
		bucket: ratelimit.NewBucket(requestsInterval,
			int64(maxRequestBurst)),
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
