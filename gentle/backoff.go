package gentle

import (
	"github.com/benbjohnson/clock"
	"github.com/cenkalti/backoff"
	"time"
)

// BackOffFactory creates a BackOff every time when RetryStream.Get()
// or RetryHandler.Handle() needs a series of back-offs.
type BackOffFactory interface {
	NewBackOff() BackOff
}

const (
	// BackOffStop is a sentinel return by BackOff.Next() indicating that no
	// more retry should be made.
	BackOffStop time.Duration = -1

	// DefaultMaxNumBackOffs is the default for ContantBackOffFactoryOpts.MaxNumBackOffs
	// and ExpBackOffFactoryOpts.MaxNumBackOffs
	DefaultMaxNumBackOffs = 0

	// DefaultRandomizationFactor is the default for ExpBackOffFactoryOpts.RandomizationFactor
	DefaultRandomizationFactor = 0.5
)

// BackOff provides a series of back-offs. It should be treated one-off. That
// is, when seeing the end(BackOffStop), it must no longer be used. If
// necessary, ask BackOffFactory to get a new one.
type BackOff interface {
	// Next() returns duration-to-wait.
	Next() time.Duration
}

// ConstBackOffFactoryOpts is settings for ConstBackOffFactory.
type ConstBackOffFactoryOpts struct {
	Interval time.Duration

	// After MaxElapsedTime or MaxNumBackOffs(whichever comes first) the BackOff
	// stops.
	// If both are 0, it never stops backing off.
	// If only one of them is 0, then the other is checked.
	MaxElapsedTime time.Duration
	MaxNumBackOffs int64
	// Clock provides a way to mock time. Mainly used in tests.
	Clock clock.Clock
}

// NewConstBackOffFactoryOpts creates a default ConstBackOffFactoryOpts.
func NewConstBackOffFactoryOpts(interval time.Duration,
	maxElapsedTime time.Duration) *ConstBackOffFactoryOpts {
	return &ConstBackOffFactoryOpts{
		Interval:       interval,
		MaxElapsedTime: maxElapsedTime,
		MaxNumBackOffs: DefaultMaxNumBackOffs,
		Clock:          clock.New(),
	}
}

// ConstBackOffFactory creates non-thread-safe constant BackOffs.
type ConstBackOffFactory struct {
	interval       time.Duration
	maxElapsedTime time.Duration
	maxNumBackOffs int64
	clock          clock.Clock
}

// NewConstBackOffFactory creates a ConstBackOffFactory.
func NewConstBackOffFactory(opts *ConstBackOffFactoryOpts) *ConstBackOffFactory {
	if opts.Interval < 0 || opts.MaxElapsedTime < 0 || opts.MaxNumBackOffs < 0 {
		panic("Invalid arguments")
	}
	return &ConstBackOffFactory{
		interval:       opts.Interval,
		maxElapsedTime: opts.MaxElapsedTime,
		maxNumBackOffs: opts.MaxNumBackOffs,
		clock:          opts.Clock,
	}
}

// NewBackOff creates a constant BackOff object.
func (f *ConstBackOffFactory) NewBackOff() BackOff {
	remainingBackOffs := int64(-1)
	if f.maxNumBackOffs > 0 {
		remainingBackOffs = f.maxNumBackOffs
	}
	return &constBackOff{
		interval:          f.interval,
		maxElapsedTime:    f.maxElapsedTime,
		remainingBackOffs: remainingBackOffs,
		clock:             f.clock,
	}
}

type constBackOff struct {
	interval          time.Duration
	maxElapsedTime    time.Duration
	remainingBackOffs int64
	clock             clock.Clock
	startTime         time.Time
}

func (b *constBackOff) getElapsedTime() time.Duration {
	return b.clock.Now().Sub(b.startTime)
}

func (b *constBackOff) Next() time.Duration {
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

// ExpBackOffFactoryOpts is settings for ExpBackOffFactory.
type ExpBackOffFactoryOpts struct {
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
	// Clock provides a way to mock time. Mainly used in tests.
	Clock clock.Clock
}

// NewExpBackOffFactoryOpts creates a default ExpBackOffFactoryOpts.
func NewExpBackOffFactoryOpts(initInterval time.Duration,
	multiplier float64, maxInterval time.Duration, maxElapsedTime time.Duration) *ExpBackOffFactoryOpts {
	return &ExpBackOffFactoryOpts{
		InitialInterval:     initInterval,
		RandomizationFactor: DefaultRandomizationFactor,
		Multiplier:          multiplier,
		MaxInterval:         maxInterval,
		MaxElapsedTime:      maxElapsedTime,
		MaxNumBackOffs:      DefaultMaxNumBackOffs,
		Clock:               clock.New(),
	}
}

// ExpBackOffFactory creates non-thread-safe exponential BackOffs.
type ExpBackOffFactory struct {
	initialInterval     time.Duration
	randomizationFactor float64
	multiplier          float64
	maxInterval         time.Duration
	maxElapsedTime      time.Duration
	maxNumBackOffs      int64
	clock               clock.Clock
}

// NewExpBackOffFactory creates a ExpBackOffFactory
func NewExpBackOffFactory(opts *ExpBackOffFactoryOpts) *ExpBackOffFactory {
	if opts.InitialInterval < 0 ||
		opts.RandomizationFactor < 0 || opts.RandomizationFactor > 1 ||
		opts.Multiplier <= 0 || opts.MaxInterval <= 0 ||
		opts.MaxElapsedTime < 0 || opts.MaxNumBackOffs < 0 {
		panic("Invalid arguments")
	}
	return &ExpBackOffFactory{
		initialInterval:     opts.InitialInterval,
		randomizationFactor: opts.RandomizationFactor,
		multiplier:          opts.Multiplier,
		maxInterval:         opts.MaxInterval,
		maxElapsedTime:      opts.MaxElapsedTime,
		maxNumBackOffs:      opts.MaxNumBackOffs,
		clock:               opts.Clock,
	}
}

// NewBackOff creates an exponential BackOff object.
func (f *ExpBackOffFactory) NewBackOff() BackOff {
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
	return &expBackOff{
		backOff:           b,
		started:           false,
		remainingBackOffs: remainingBackOffs,
	}
}

type expBackOff struct {
	backOff           *backoff.ExponentialBackOff
	started           bool
	remainingBackOffs int64
}

func (b *expBackOff) Next() time.Duration {
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
