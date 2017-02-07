// vim:fileencoding=utf-8
package service

import (
	"github.com/inconshreveable/log15"
	"time"
	"errors"
	"github.com/rs/xid"
)

var Log = log15.New()

func init()  {
	Log.SetHandler(log15.DiscardHandler())
}

type BackOff interface {
	Run(work func() error) error
}

type UpStream interface {
	SetBackPressure(Monitor, BackOff) error
	Run()
	// timeout == 0 results in blocking as long as it needs.
	WaitMessage(time.Duration) (interface{}, error)
}

type DownStream interface {
	Run(UpStream, func(interface{}) error) error
}

type Monitor interface {
	NeedBackOff() bool
}

type Message interface {
	Id() string
}

type Sender interface {
	// timeout == 0 results in blocking as long as it needs.
	SendMessage(Message, time.Duration) (Message, error)
	Logger() log15.Logger
}

// A rate limit.
type RateLimit interface {
	// Wait for $count tokens are granted(return true) or
	// timeout(return false).
	// timeout == 0 results in blocking as long as it needs.
	Wait(int64, time.Duration) bool
}

var ErrRateLimited = errors.New("Rate limit reached")
var ErrTimeout = errors.New("Timeout")
var ErrBackOff = errors.New("Should back off")

type Msg struct {
	id string
}

func NewMsg() *Msg {
	return &Msg{
		id: xid.New().String(),
	}
}

func (m *Msg) Id() string {
	return m.id
}

func IntToMillis(millis int) time.Duration {
	return time.Duration(millis) * time.Millisecond
}
