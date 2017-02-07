// vim:fileencoding=utf-8
// A wrapper for "github.com/juju/ratelimit".

package service

import (
	"time"
	"github.com/juju/ratelimit"
)

type TokenBucketRateLimitConf struct {
	// The minimum interval, in milliseconds, between two consecutive
	// requests. It's used to condition the rate.
	RequestsInterval int

	// N is the amount of requests allowed when a burst of requests coming
	// in after not seeing requests for N * RequestsInterval. N is capped by
	// MaxRequestsBurst.
	// If N == 1, then no burst allowed and requests are served at a steady
	// rate.
	MaxRequestsBurst int64
}

type TokenBucketRateLimit struct {
	bucket *ratelimit.Bucket
}

func NewTokenBucketRateLimit(conf TokenBucketRateLimitConf) *TokenBucketRateLimit {
	return &TokenBucketRateLimit{
		bucket: ratelimit.NewBucket(
			IntToMillis(conf.RequestsInterval),
			conf.MaxRequestsBurst),
	}
}

func (rl *TokenBucketRateLimit) Wait(count int64, timeout time.Duration) bool {
	if timeout == 0 {
		rl.bucket.Wait(count)
		return true
	}
	return rl.bucket.WaitMaxDuration(count, timeout)
}

