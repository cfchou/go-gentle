// vim:fileencoding=utf-8
package service

import (
	"github.com/inconshreveable/log15"
	"time"
	"errors"
)

var Log = log15.New()

func init()  {
	Log.SetHandler(log15.DiscardHandler())
}

type Message interface {
	Id() string
}

type Messages interface {
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
	Exchange(msg Message, timeout time.Duration) (Messages, error)
	Logger() log15.Logger
}

// Receiver is built on top of Driver with the following features:
// 1. One Receiver.Receive() fetches one Message for downstream.
// 2. Receiver.Receive() triggers Driver.Exchange() lazily.
//
// No timeout is specified for Receiver.Receive() because it should block until
// a Message for downstream is available.
type Receiver interface {
	Receive() (Message, error)
	Logger() log15.Logger
}

type MessageSource interface {
	ReceiveMessages() ([]Message, error)
}

// UpStream provides an interface for DownStream to consume one Message a time.
// A DownStream applies back pressure to UpStream per Message basis which may in turn back pressure
// the Receiver.
type UpStream interface {
	//Run() error
	// timeout == 0 results in blocking as long as it needs.
	WaitMessage(time.Duration) (Message, error)
}

type Handler func(Message) (Message, error)

type DownStream interface {
	Run(UpStream, Handler) error
}

type Sender interface {
	// timeout == 0 results in blocking as long as it needs.
	SendMessage(Message, time.Duration) (Message, error)
	Logger() log15.Logger
}

/*
type BackOff interface {
	Run(work func() error) error
	/*
	Wait()
	Reset()
	*/
}
*/

type BackOff interface {
	Wait()
	WaitUntil(timeout time.Duration) bool
	UpdateWith(when time.Time, err error)
}

type Monitor interface {
	NeedBackOff() bool
}

type RateLimit interface {
	// Wait for $count tokens are granted(return true) or
	// timeout(return false).
	// timeout == 0 results in blocking as long as it needs.
	Wait(int64, time.Duration) bool
}

const (
	created                = iota
	running                = iota
)

var ErrRateLimited = errors.New("Rate limit reached")
var ErrTimeout = errors.New("Timeout")
var ErrBackOff = errors.New("Should back off")
var ErrConf = errors.New("Config error")
var ErrRepeatedRun = errors.New("Repeated run")
var ErrUpStream = errors.New("Error from upstream")

func IntToMillis(millis int) time.Duration {
	return time.Duration(millis) * time.Millisecond
}
