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

type MessageSource interface {
	ReceiveMessages() ([]Message, error)
}

type UpStream interface {
	Run() error
	// timeout == 0 results in blocking as long as it needs.
	WaitMessage(time.Duration) (Message, error)
}

type Handler func(Message) error

type DownStream interface {
	Run(UpStream, Handler) error
}

type Sender interface {
	// timeout == 0 results in blocking as long as it needs.
	SendMessage(Message, time.Duration) (Message, error)
	Logger() log15.Logger
}

type BackOff interface {
	Run(work func() error) error
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

func IntToMillis(millis int) time.Duration {
	return time.Duration(millis) * time.Millisecond
}
