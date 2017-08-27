package gentle

import (
	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestConstBackOffFactory_NewBackOff_MaxElapsedTime(t *testing.T) {
	// MaxElapsedTime > 0 && MaxNumBackOffs == 0
	// Stops when reached MaxElapsedTime
	mclock := clock.NewMock()
	opts := NewConstBackOffFactoryOpts(time.Second, 5*time.Second)
	opts.Clock = mclock
	factory := NewConstBackOffFactory(opts)
	backoff := factory.NewBackOff()
	assert.True(t, opts.Interval == backoff.Next())
	// Should stop after MaxElapsedTime
	mclock.Add(opts.MaxElapsedTime + time.Duration(1))
	assert.True(t, BackOffStop == backoff.Next())
}

func TestConstBackOffFactory_NewBackOff_MaxNumBackOffs(t *testing.T) {
	// MaxElapsedTime == 0 && MaxNumBackOffs > 0
	// Stops when reached MaxNumBackOffs
	opts := NewConstBackOffFactoryOpts(time.Second, 0)
	opts.MaxNumBackOffs = 10
	factory := NewConstBackOffFactory(opts)
	backoff := factory.NewBackOff()
	for i := int64(0); i < opts.MaxNumBackOffs; i++ {
		assert.True(t, opts.Interval == backoff.Next())
	}
	// Should stop after MaxNumBackOffs
	assert.True(t, BackOffStop == backoff.Next())
}

func TestConstBackOffFactory_Forever(t *testing.T) {
	// Run forever when both MaxElapsedTime and MaxNumBackOffs are 0
	mclock := clock.NewMock()
	opts := NewConstBackOffFactoryOpts(time.Second, 0)
	opts.MaxNumBackOffs = 0
	opts.Clock = mclock
	factory := NewConstBackOffFactory(opts)
	backoff := factory.NewBackOff()
	assert.True(t, opts.Interval == backoff.Next())
	mclock.Add(time.Hour)
	assert.True(t, opts.Interval == backoff.Next())
}

func TestExpBackOffFactory_MaxElapsedTime(t *testing.T) {
	// MaxElapsedTime > 0 && MaxNumBackOffs == 0
	// Stops when reached MaxElapsedTime
	mclock := clock.NewMock()
	opts := NewExpBackOffFactoryOpts(time.Second, 1, time.Second,
		5*time.Second)
	opts.Clock = mclock
	factory := NewExpBackOffFactory(opts)
	backoff := factory.NewBackOff()
	assert.False(t, BackOffStop == backoff.Next())
	// Should stop after MaxElapsedTime
	mclock.Add(opts.MaxElapsedTime + time.Duration(1))
	assert.True(t, BackOffStop == backoff.Next())
}

func TestExpBackOffFactory_MaxBackOffs(t *testing.T) {
	// MaxElapsedTime == 0 && MaxNumBackOffs > 0
	// Stops when reached MaxNumBackOffs
	opts := NewExpBackOffFactoryOpts(time.Second, 1, time.Second, 0)
	opts.MaxNumBackOffs = 10
	factory := NewExpBackOffFactory(opts)
	backoff := factory.NewBackOff()
	for i := int64(0); i < opts.MaxNumBackOffs; i++ {
		assert.False(t, BackOffStop == backoff.Next())
	}
	// Should stop after MaxNumBackOffs
	assert.True(t, BackOffStop == backoff.Next())
}

func TestExpBackOffFactory_Forever(t *testing.T) {
	// MaxElapsedTime == 0 && MaxNumBackOffs > 0
	// Stops when reached MaxNumBackOffs
	mclock := clock.NewMock()
	opts := NewExpBackOffFactoryOpts(time.Second, 1, time.Second, 0)
	opts.MaxNumBackOffs = 0
	opts.Clock = mclock
	factory := NewExpBackOffFactory(opts)
	backoff := factory.NewBackOff()
	assert.True(t, BackOffStop != backoff.Next())
	mclock.Add(time.Hour)
	assert.True(t, BackOffStop != backoff.Next())
}
