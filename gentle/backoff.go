package gentle

import (
	"time"
	"github.com/cenkalti/backoff"
	"github.com/benbjohnson/clock"
)

const BackOffStop time.Duration = -1

type ConstantBackOffFactoryOpts struct {
	Interval  time.Duration
	// After MaxElapsedTime the BackOff stops.
	// It never stops if MaxElapsedTime == 0.
	MaxElapsedTime time.Duration
	Clock clock.Clock
}

func NewConstantBackOffFactoryOpts(interval time.Duration,
	maxElapsedTime time.Duration) *ConstantBackOffFactoryOpts {

	return &ConstantBackOffFactoryOpts{
		Interval: interval,
		MaxElapsedTime: maxElapsedTime,
		Clock: clock.New(),
	}
}

type ConstantBackOffFactory struct {
	interval  time.Duration
	maxElapsedTime time.Duration
	clock clock.Clock
}

func NewConstantBackOffFactory(opts ConstantBackOffFactoryOpts) *ConstantBackOffFactory {
	return &ConstantBackOffFactory{
		interval: opts.Interval,
		maxElapsedTime: opts.MaxElapsedTime,
		clock: opts.Clock,
	}
}

func (f *ConstantBackOffFactory) NewBackOff() BackOff {
	return &constantBackOff{
		backOff: backoff.NewConstantBackOff(f.interval),
		maxElapsedTime: f.maxElapsedTime,
		clock: f.clock,
		startTime: f.clock.Now(),
	}
}

type constantBackOff struct {
	backOff *backoff.ConstantBackOff
	maxElapsedTime time.Duration
	clock clock.Clock
	startTime       time.Time
}

func (b *constantBackOff) getElapsedTime() time.Duration {
	return b.clock.Now().Sub(b.startTime)
}

func (b *constantBackOff) Next() time.Duration {
	if b.maxElapsedTime != 0 && b.getElapsedTime() > b.maxElapsedTime {
		return BackOffStop
	}
	next := b.backOff.NextBackOff()
	if next == backoff.Stop {
		// should never happen
		return BackOffStop
	}
	return next
}

type ExponentialBackoffFactoryOpts struct {
	// Next() returns a randomizedInterval which is:
	// currentInterval * rand(range [1-RandomizationFactor, 1+RandomizationFactor])
	InitialInterval     time.Duration
	RandomizationFactor float64
	Multiplier          float64

	// If currentInterval * Multiplier >= MaxInterval, then currentInterval = b.MaxInterval
	// Otherwise, currentInterval *= Multiplier
	MaxInterval         time.Duration

	// After MaxElapsedTime the ExponentialBackOff stops.
	// It never stops if MaxElapsedTime == 0.
	MaxElapsedTime time.Duration
	Clock    clock.Clock
}

func NewExponentialBackoffFactoryOpts(initInterval time.Duration,
	multiplier float64, maxInterval time.Duration, maxElapsedTime time.Duration) *ExponentialBackoffFactoryOpts {
	return &ExponentialBackoffFactoryOpts{
		InitialInterval: initInterval,
		RandomizationFactor: 0.5,
		Multiplier: 	multiplier,
		MaxInterval: maxInterval,
		MaxElapsedTime: maxElapsedTime,
	}
}

type ExponentialBackoffFactory struct {
	initialInterval     time.Duration
	randomizationFactor float64
	multiplier          float64
	maxInterval         time.Duration
	maxElapsedTime time.Duration
	clock    clock.Clock
}

func NewExponentialBackoffFactory(opts ExponentialBackoffFactoryOpts) *ExponentialBackoffFactory {
	return &ExponentialBackoffFactory{
		initialInterval: opts.InitialInterval,
		randomizationFactor: opts.RandomizationFactor,
		multiplier: opts.Multiplier,
		maxInterval: opts.MaxInterval,
		maxElapsedTime: opts.MaxElapsedTime,
		clock: opts.Clock,
	}

}

func (f *ExponentialBackoffFactory) NewBackOff() BackOff {
	b := &backoff.ExponentialBackOff{
		InitialInterval:     f.initialInterval,
		RandomizationFactor: f.randomizationFactor,
		Multiplier:          f.multiplier,
		MaxInterval:         f.maxInterval,
		MaxElapsedTime:      f.maxElapsedTime,
		Clock:               f.clock,
	}
	if b.RandomizationFactor < 0 {
		b.RandomizationFactor = 0
	} else if b.RandomizationFactor > 1 {
		b.RandomizationFactor = 1
	}
	b.Reset()
	return &exponentialBackOff{
		backOff: b,
	}
}

type exponentialBackOff struct {
	backOff *backoff.ExponentialBackOff
}

func (b *exponentialBackOff) Next() time.Duration {
	next := b.backOff.NextBackOff()
	if next == backoff.Stop {
		return BackOffStop
	}
	return next
}


