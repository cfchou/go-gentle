package gentle

import (
	"time"
	"github.com/cenkalti/backoff"
)

type ConstantBackOffFactoryOpts struct {
	Interval  time.Duration
	MaxNextTimes int
}

type ConstantBackOffFactory struct {
	interval  time.Duration
	maxNextTimes int
}

func (f *ConstantBackOffFactory) NewBackOff() BackOff {
	return &constantBackOff{
		backOff: backoff.NewConstantBackOff(f.interval),
		maxNextTimes: f.maxNextTimes,
	}
}

// It's a one-shot backoff.
// If maxNextTimes is less than or equal to 0, then Next() will never return
// BackOffStop. Otherwise, Next() will always return BackOffStop if maxNextTimes
// of Next() is reached.
type constantBackOff struct {
	backOff *backoff.ConstantBackOff
	maxNextTimes int
	nextTimes int
}

func (b *constantBackOff) Next() time.Duration {
	if b.maxNextTimes <= 0 {
		return b.backOff.NextBackOff()
	}
	if b.nextTimes + 1 > b.maxNextTimes {
		return BackOffStop
	}
	b.nextTimes++
	return b.backOff.NextBackOff()
}

type ExponentialBackoffFactoryOpts struct {

}

type ExponentialBackoffFactory struct {

}

type exponentialBackOff struct {
	backOff *backoff.ExponentialBackOff
}


func (b *exponentialBackOff) Next() time.Duration {
	return b.backOff.NextBackOff()
}


