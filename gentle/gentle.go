package gentle

import (
	"github.com/afex/hystrix-go/hystrix"
	"github.com/inconshreveable/log15"
	"time"
)

// Package level logger. It uses log15(https://godoc.org/github.com/inconshreveable/log15)
// to provide finer control over logging.
var Log = log15.New()

func init() {
	Log.SetHandler(log15.DiscardHandler())
}

type Message interface {
	// A Message is obliged to implement Id() for better tracing.
	Id() string
}

// Stream emits Message. Messages of a stream goes one way. Though two streams
// can simulate two-way communication but it would require out-of-band logic.
type Stream interface {
	// Get() returns either a Message or an error. Returned Message is nil
	// if only if error is not nil.
	Get() (Message, error)
}

// Handler transforms a Message.
type Handler interface {
	// Handle() transforms an Message. Returned Message is nil if only if
	// error is not nil.
	Handle(msg Message) (Message, error)
}

// RateLimit is an interface for a "token bucket" algorithm.
type RateLimit interface {
	// Wait for $count tokens are granted(return true) or timeout(return
	// false). If $timeout == 0, it would block as long as it needs.
	Wait(count int, timeout time.Duration) bool
}

// Converts $millis of int to time.Duration.
func IntToMillis(millis int) time.Duration {
	return time.Duration(millis) * time.Millisecond
}

// GetHystrixDefaultConfig() returns a new hystrix.CommandConfig filled with defaults(https://godoc.org/github.com/afex/hystrix-go/hystrix#pkg-variables):
func GetHystrixDefaultConfig() *hystrix.CommandConfig {
	return &hystrix.CommandConfig{
		Timeout: hystrix.DefaultTimeout,
		MaxConcurrentRequests: hystrix.DefaultMaxConcurrent,
		RequestVolumeThreshold: hystrix.DefaultVolumeThreshold,
		SleepWindow: hystrix.DefaultSleepWindow,
		ErrorPercentThreshold: hystrix.DefaultErrorPercentThreshold,
	}
}

type tuple struct {
	fst interface{}
	snd interface{}
}

// GenBackOff is a function returning a slice of time.Duration that will be
// used as intervals between back-offs.
type GenBackOff func() []time.Duration