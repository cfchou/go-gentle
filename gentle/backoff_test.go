package gentle

import (
	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestConstantBackOffFactory_NewBackOff_MaxElapsedTime(t *testing.T) {
	// MaxElapsedTime > 0 && MaxNumBackOffs == 0
	// Stops when reached MaxElapsedTime
	mclock := clock.NewMock()
	opts := NewConstantBackOffFactoryOpts(time.Second, 5*time.Second)
	opts.Clock = mclock
	factory := NewConstantBackOffFactory(opts)
	backoff := factory.NewBackOff()
	assert.True(t, opts.Interval == backoff.Next())
	// Should stop after MaxElapsedTime
	mclock.Add(opts.MaxElapsedTime + time.Duration(1))
	assert.True(t, BackOffStop == backoff.Next())
}

func TestConstantBackOffFactory_NewBackOff_MaxNumBackOffs(t *testing.T) {
	// MaxElapsedTime == 0 && MaxNumBackOffs > 0
	// Stops when reached MaxNumBackOffs
	opts := NewConstantBackOffFactoryOpts(time.Second, 0)
	opts.MaxNumBackOffs = 10
	factory := NewConstantBackOffFactory(opts)
	backoff := factory.NewBackOff()
	for i := int64(0); i < opts.MaxNumBackOffs; i++ {
		assert.True(t, opts.Interval == backoff.Next())
	}
	// Should stop after MaxNumBackOffs
	assert.True(t, BackOffStop == backoff.Next())
}

func TestConstantBackOffFactory_Forever(t *testing.T) {
	// Run forever when both MaxElapsedTime and MaxNumBackOffs are 0
	mclock := clock.NewMock()
	opts := NewConstantBackOffFactoryOpts(time.Second, 0)
	opts.MaxNumBackOffs = 0
	opts.Clock = mclock
	factory := NewConstantBackOffFactory(opts)
	backoff := factory.NewBackOff()
	assert.True(t, opts.Interval == backoff.Next())
	mclock.Add(time.Hour)
	assert.True(t, opts.Interval == backoff.Next())
}

func TestExponentialBackOffFactory_MaxElapsedTime(t *testing.T) {
	// MaxElapsedTime > 0 && MaxNumBackOffs == 0
	// Stops when reached MaxElapsedTime
	mclock := clock.NewMock()
	opts := NewExponentialBackOffFactoryOpts(time.Second, 1, time.Second,
		5*time.Second)
	opts.Clock = mclock
	factory := NewExponentialBackOffFactory(opts)
	backoff := factory.NewBackOff()
	assert.False(t, BackOffStop == backoff.Next())
	// Should stop after MaxElapsedTime
	mclock.Add(opts.MaxElapsedTime + time.Duration(1))
	assert.True(t, BackOffStop == backoff.Next())
}

func TestExponentialBackOffFactory_MaxBackOffs(t *testing.T) {
	// MaxElapsedTime == 0 && MaxNumBackOffs > 0
	// Stops when reached MaxNumBackOffs
	opts := NewExponentialBackOffFactoryOpts(time.Second, 1, time.Second, 0)
	opts.MaxNumBackOffs = 10
	factory := NewExponentialBackOffFactory(opts)
	backoff := factory.NewBackOff()
	for i := int64(0); i < opts.MaxNumBackOffs; i++ {
		assert.False(t, BackOffStop == backoff.Next())
	}
	// Should stop after MaxNumBackOffs
	assert.True(t, BackOffStop == backoff.Next())
}

func TestExponentialBackOffFactory_Forever(t *testing.T) {
	// MaxElapsedTime == 0 && MaxNumBackOffs > 0
	// Stops when reached MaxNumBackOffs
	mclock := clock.NewMock()
	opts := NewExponentialBackOffFactoryOpts(time.Second, 1, time.Second, 0)
	opts.MaxNumBackOffs = 0
	opts.Clock = mclock
	factory := NewExponentialBackOffFactory(opts)
	backoff := factory.NewBackOff()
	assert.True(t, BackOffStop != backoff.Next())
	mclock.Add(time.Hour)
	assert.True(t, BackOffStop != backoff.Next())
}
