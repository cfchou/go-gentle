package gentle

import (
	"github.com/benbjohnson/clock"
	"github.com/cenkalti/backoff"
	"time"
)

// BackOffFactory creates one-off BackOff objects for RetryStream.Get() and
// RetryHandler.Handle().
type BackOffFactory interface {
	NewBackOff() BackOff
}

// BackOffStop is a sentinel that BackOff.Next() should return to stop backing
// off.
const BackOffStop time.Duration = -1

// BackOff provides durations of back-offs.
type BackOff interface {
	// Next() returns duration-to-wait.
	Next() time.Duration
}

type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
}

// ConstantBackOffFactoryOpts is options for ConstantBackOffFactory.
type ConstantBackOffFactoryOpts struct {
	Interval time.Duration

	// After MaxElapsedTime or MaxNumBackOffs(whichever comes first) the BackOff
	// stops.
	// If both are 0, it never stops backing off.
	// If only one of them is 0, then the other is checked.
	MaxElapsedTime time.Duration
	MaxNumBackOffs int64
	Clock          Clock
}

// NewConstantBackOffFactoryOpts creates a default ConstantBackOffFactoryOpts.
func NewConstantBackOffFactoryOpts(interval time.Duration,
	maxElapsedTime time.Duration) *ConstantBackOffFactoryOpts {
	return &ConstantBackOffFactoryOpts{
		Interval:       interval,
		MaxElapsedTime: maxElapsedTime,
		MaxNumBackOffs: 0,
		Clock:          clock.New(),
	}
}

// ConstantBackOffFactory creates non-thread-safe constant BackOff objects.
type ConstantBackOffFactory struct {
	interval       time.Duration
	maxElapsedTime time.Duration
	maxNumBackOffs int64
	clock          Clock
}

// NewConstantBackOffFactory creates ConstantBackOffFactory's
func NewConstantBackOffFactory(opts *ConstantBackOffFactoryOpts) *ConstantBackOffFactory {
	if opts.Interval < 0 || opts.MaxElapsedTime < 0 || opts.MaxNumBackOffs < 0 {
		panic("Invalid arguments")
	}
	return &ConstantBackOffFactory{
		interval:       opts.Interval,
		maxElapsedTime: opts.MaxElapsedTime,
		maxNumBackOffs: opts.MaxNumBackOffs,
		clock:          opts.Clock,
	}
}

func (f *ConstantBackOffFactory) NewBackOff() BackOff {
	remainingBackOffs := int64(-1)
	if f.maxNumBackOffs > 0 {
		remainingBackOffs = f.maxNumBackOffs
	}
	return &constantBackOff{
		interval:          f.interval,
		maxElapsedTime:    f.maxElapsedTime,
		remainingBackOffs: remainingBackOffs,
		clock:             f.clock,
	}
}

type constantBackOff struct {
	interval          time.Duration
	maxElapsedTime    time.Duration
	remainingBackOffs int64
	clock             Clock
	startTime         time.Time
}

func (b *constantBackOff) getElapsedTime() time.Duration {
	return b.clock.Now().Sub(b.startTime)
}

func (b *constantBackOff) Next() time.Duration {
	if b.startTime.IsZero() {
		b.startTime = b.clock.Now()
	}
	if b.maxElapsedTime != 0 && b.getElapsedTime() > b.maxElapsedTime {
		return BackOffStop
	}
	if b.remainingBackOffs == 0 {
		return BackOffStop
	}
	b.remainingBackOffs--
	return b.interval
}

type ExponentialBackOffFactoryOpts struct {
	// Next() returns a randomizedInterval which is:
	// currentInterval * rand(range [1-RandomizationFactor, 1+RandomizationFactor])
	InitialInterval     time.Duration
	RandomizationFactor float64
	Multiplier          float64

	// If currentInterval * Multiplier >= MaxInterval, then currentInterval = b.MaxInterval
	// Otherwise, currentInterval *= Multiplier
	MaxInterval time.Duration

	// After MaxElapsedTime or MaxNumBackOffs(whichever comes first) the BackOff
	// stops.
	// If both are 0, it never stops backing off.
	// If only one of them is 0, then the other is checked.
	MaxElapsedTime time.Duration
	MaxNumBackOffs int64
	Clock          Clock
}

func NewExponentialBackOffFactoryOpts(initInterval time.Duration,
	multiplier float64, maxInterval time.Duration, maxElapsedTime time.Duration) *ExponentialBackOffFactoryOpts {
	return &ExponentialBackOffFactoryOpts{
		InitialInterval:     initInterval,
		RandomizationFactor: 0.5,
		Multiplier:          multiplier,
		MaxInterval:         maxInterval,
		MaxElapsedTime:      maxElapsedTime,
		MaxNumBackOffs:      0,
		Clock:               clock.New(),
	}
}

// ExponentialBackOffFactory creates non-thread-safe exponential BackOff objects
type ExponentialBackOffFactory struct {
	initialInterval     time.Duration
	randomizationFactor float64
	multiplier          float64
	maxInterval         time.Duration
	maxElapsedTime      time.Duration
	maxNumBackOffs      int64
	clock               Clock
}

func NewExponentialBackOffFactory(opts *ExponentialBackOffFactoryOpts) *ExponentialBackOffFactory {
	if opts.InitialInterval < 0 ||
		opts.RandomizationFactor < 0 || opts.RandomizationFactor > 1 ||
		opts.Multiplier <= 0 || opts.MaxInterval <= 0 ||
		opts.MaxElapsedTime < 0 || opts.MaxNumBackOffs < 0 {
		panic("Invalid arguments")
	}
	return &ExponentialBackOffFactory{
		initialInterval:     opts.InitialInterval,
		randomizationFactor: opts.RandomizationFactor,
		multiplier:          opts.Multiplier,
		maxInterval:         opts.MaxInterval,
		maxElapsedTime:      opts.MaxElapsedTime,
		maxNumBackOffs:      opts.MaxNumBackOffs,
		clock:               opts.Clock,
	}

}

func (f *ExponentialBackOffFactory) NewBackOff() BackOff {
	remainingBackOffs := int64(-1)
	if f.maxNumBackOffs > 0 {
		remainingBackOffs = f.maxNumBackOffs
	}
	b := &backoff.ExponentialBackOff{
		InitialInterval:     f.initialInterval,
		RandomizationFactor: f.randomizationFactor,
		Multiplier:          f.multiplier,
		MaxInterval:         f.maxInterval,
		MaxElapsedTime:      f.maxElapsedTime,
		Clock:               f.clock,
	}
	//b.Reset()
	return &exponentialBackOff{
		backOff:           b,
		started:           false,
		remainingBackOffs: remainingBackOffs,
	}
}

type exponentialBackOff struct {
	backOff           *backoff.ExponentialBackOff
	started           bool
	remainingBackOffs int64
}

func (b *exponentialBackOff) Next() time.Duration {
	if !b.started {
		b.backOff.Reset()
		b.started = true
	}
	if b.backOff.MaxElapsedTime == 0 && b.remainingBackOffs == 0 {
		return BackOffStop
	}
	b.remainingBackOffs--
	next := b.backOff.NextBackOff()
	if next == backoff.Stop {
		return BackOffStop
	}
	return next
}
