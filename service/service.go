// vim:fileencoding=utf-8
package service

import (
	"github.com/inconshreveable/log15"
	"time"
	"errors"
)

// Package level logger.
var Log = log15.New()

func init()  {
	Log.SetHandler(log15.DiscardHandler())
}

type Message interface {
	Id() string
}

type MetaMessage interface {
	Message
	Flatten() []Message
}

// Driver.Exchange() directly maps to a request to an external service.
// The response of such request may map to multiple Message for downstream.
type Driver interface {
	// TODO: consider using context package for timeout
	// timeout 0 results in blocking as long as it needs.
	// It's important to know that if Exchange() comes back with
	// ErrTimeout, depending on the implementation, the msg could still
	// be delivered(at-most-once).
	Exchange(msg Message, timeout time.Duration) (MetaMessage, error)
	Logger() log15.Logger
}

// Receiver is built on top of Driver with the following features(differences):
// 1. One Receiver.Receive() fetches one Message for downstream whilst a Driver
//    fetches one MetaMessage which may Flatten() to many Message's.
// 2. Receiver.Receive() triggers Driver.Exchange($msg_in) lazily. Most often,
//    $msg_in is a dummy value that the Driver would just ignore.
// 3. Messages passed to both ways of a Diver are interleaved so the order is
//    automatically maintained.
// 4. Messages of a stream goes one way. Though two streams can simulate
//    two-way communication but it would require out-of-band logic to maintain
//    the order.
// 5. No timeout is specified for Receiver.Receive() because it should block
//    until a Message for downstream is available.
type Stream interface {
	Receive() (Message, error)
	Logger() log15.Logger
}

// Handler reacts to Message from Stream.
type Handler interface {
	Handle(Message) (Message, error)
	Logger() log15.Logger
}

// RateLimit is an interface for a "token bucket" rate limit algorithm.
type RateLimit interface {
	// Wait for $count tokens are granted(return true) or
	// timeout(return false).
	// timeout == 0 results in blocking as long as it needs.
	Wait(int64, time.Duration) bool
}

var ErrEOF = errors.New("EOF")
var ErrRateLimited = errors.New("Rate limit reached")
var ErrTimeout = errors.New("Timeout")
var ErrConf = errors.New("Config error")
var ErrUpStream = errors.New("Error from upstream")

func IntToMillis(millis int) time.Duration {
	return time.Duration(millis) * time.Millisecond
}

type tuple struct {
	fst interface{}
	snd interface{}
}

type GenBackOff func() []time.Duration

