// vim:fileencoding=utf-8
package service

import (
	"github.com/inconshreveable/log15"
	"time"
	"github.com/aws/aws-sdk-go/service/sqs"
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
	WaitMessage(time.Duration) (interface{}, error)
}

type DownStream interface {
	Run(UpStream, func(interface{}) error) error
}

type Monitor interface {
	NeedBackOff() bool
}

type Sender interface {
	SendMessage(interface{}, time.Duration) (interface{}, error)
	GetLogger() log15.Logger
}

type SendSpec interface {
	ToSendMessageInput() (*sqs.SendMessageInput, error)
}

// A rate limit.
type RateLimit interface {
	// Wait for $count tokens are granted(return true) or
	// timeout(return false).
	Wait(int64, time.Duration) bool
}

