// vim:fileencoding=utf-8
package service

import (
	"github.com/inconshreveable/log15"
	"time"
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
	// returned Message is nil if only if error is not nil
	Receive() (Message, error)
}

// Handler reacts to Message from Stream.
type Handler interface {
	// returned Message is nil if only if error is not nil
	Handle(Message) (Message, error)
}

// RateLimit is an interface for a "token bucket" rate limit algorithm.
type RateLimit interface {
	// Wait for $count tokens are granted(return true) or
	// timeout(return false).
	// timeout == 0 results in blocking as long as it needs.
	Wait(int64, time.Duration) bool
}

func IntToMillis(millis int) time.Duration {
	return time.Duration(millis) * time.Millisecond
}

type tuple struct {
	fst interface{}
	snd interface{}
}

type GenBackOff func() []time.Duration
