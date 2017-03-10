// vim:fileencoding=utf-8
package service

import (
	"github.com/inconshreveable/log15"
	"time"
	"github.com/afex/hystrix-go/hystrix"
)

// Package level logger.
var Log = log15.New()

func init() {
	Log.SetHandler(log15.DiscardHandler())
}

type Message interface {
	Id() string
}

// Messages of a stream goes one way. Though two streams can simulate two-way
// communication but it would require out-of-band logic.
type Stream interface {
	// Receive() returns either a Message or an error. Returned Message is
	// nil if only if error is not nil.
	Receive() (Message, error)
}

// Handler reacts to Message from Stream.
type Handler interface {
	// Handle() transforms an input Message. It returns either a Message or
	// an error. Returned Message is nil if only if error is not nil.
	Handle(Message) (Message, error)
}

// RateLimit is an interface for a "token bucket" rate limit algorithm.
type RateLimit interface {
	// Wait for $count tokens are granted(return true) or
	// timeout(return false).
	// timeout == 0 would block as long as it needs.
	Wait(int, time.Duration) bool
}

func IntToMillis(millis int) time.Duration {
	return time.Duration(millis) * time.Millisecond
}

// GetHystrixDefaultConfig() returns a new CommandConfig filled with defaults.
//
// They are:
// Timeout -- How long in millis that if a request exceeds, timeout metric
// would increase, then the circuit may open.

func GetHystrixDefaultConfig() *hystrix.CommandConfig {
	return &hystrix.CommandConfig{
		// How long in millis that if a request exceeds, timeout metric
		// would increase, then the circuit may open.
		Timeout:                hystrix.DefaultTimeout,

		ErrorPercentThreshold:  hystrix.DefaultErrorPercentThreshold,

		MaxConcurrentRequests:  hystrix.DefaultMaxConcurrent,

		// the minimum number of requests in the last 10 seconds needed
		// before a circuit can be tripped.
		RequestVolumeThreshold: hystrix.DefaultVolumeThreshold,

		SleepWindow:            hystrix.DefaultSleepWindow,
	}
}

type tuple struct {
	fst interface{}
	snd interface{}
}

type GenBackOff func() []time.Duration
