// vim:fileencoding=utf-8
// A wrapper for "github.com/juju/ratelimit"

package service

import (
	"time"
	"github.com/juju/ratelimit"
)

type RateLimitConfig struct {
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

type RateLimiter struct {
	bucket *ratelimit.Bucket
}

// Create a RateLimiter. If RateLimiterConfig.RequestsInterval == 0, then an
// unlimited one is returned.
func (rc *RateLimitConfig) NewRateLimit() *RateLimiter {
	a := 10
	if rc.RequestsInterval > 0 {
		return &RateLimiter{
			bucket: ratelimit.NewBucket(
				time.Duration(rc.RequestsInterval) * time.Millisecond,
				rc.MaxRequestsBurst),
		}
	}
	// Unlimited
	return &RateLimiter{}
}

func (rl *RateLimiter) HasLimit() bool {
	return rl.bucket != nil
}

// Wait takes count tokens from the bucket, waiting until they are
// available.
func (rl *RateLimiter) Wait(count int64) {
	if !rl.HasLimit() {
		return
	}
	rl.bucket.Wait(count)
	return
}

// WaitMaxDuration is like Wait except that it will
// only take tokens from the bucket if it needs to wait
// for no greater than maxWait. It reports whether
// any tokens have been removed from the bucket
// If no tokens have been removed, it returns immediately.
func (rl *RateLimiter) WaitMaxDuration(count int64, maxWait time.Duration) bool {
	if !rl.HasLimit() {
		return true
	}
	return rl.bucket.WaitMaxDuration(count, maxWait)

}

// Take takes count tokens from the bucket without blocking. It returns
// the time that the caller should wait until the tokens are actually
// available.
//
// Note that if the request is irrevocable - there is no way to return
// tokens to the bucket once this method commits us to taking them.
func (rl *RateLimiter) Take(count int64) time.Duration {
	if !rl.HasLimit() {
		return 0 * time.Millisecond
	}
	return rl.bucket.Take(count)
}

// TakeMaxDuration is like Take, except that
// it will only take tokens from the bucket if the wait
// time for the tokens is no greater than maxWait.
//
// If it would take longer than maxWait for the tokens
// to become available, it does nothing and reports false,
// otherwise it returns the time that the caller should
// wait until the tokens are actually available, and reports
// true.
func (rl *RateLimiter) TakeMaxDuration(count int64, maxWait time.Duration) (time.Duration, bool) {
	if !rl.HasLimit() {
		return 0 * time.Millisecond, true
	}
	return rl.bucket.TakeMaxDuration(count, maxWait)
}

// TakeAvailable takes up to count immediately available tokens from the
// bucket. It returns the number of tokens removed, or zero if there are
// no available tokens. It does not block.
func (rl *RateLimiter) TakeAvailable(count int64) int64 {
	if !rl.HasLimit() {
		return count
	}
	return rl.bucket.TakeAvailable(count)
}

// Available returns the number of available tokens. It will be negative
// when there are consumers waiting for tokens. Note that if this
// returns greater than zero, it does not guarantee that calls that take
// tokens from the buffer will succeed, as the number of available
// tokens could have changed in the meantime. This method is intended
// primarily for metrics reporting and debugging.
func (rl *RateLimiter) Available() int64 {
	if !rl.HasLimit() {
		panic("has no limit")
	}
	return rl.bucket.Available()
}

// Capacity returns the capacity that the bucket was created with.
func (rl *RateLimiter) Capacity() int64 {
	if !rl.HasLimit() {
		panic("has no limit")
	}
	return rl.bucket.Capacity()
}

// Rate returns the fill rate of the bucket, in tokens per second.
func (rl *RateLimiter) Rate() float64 {
	if !rl.HasLimit() {
		panic("has no limit")
	}
	return rl.bucket.Rate()
}

