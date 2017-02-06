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
	SetBackPressure(monitor DownStreamMonitor, backOff BackOff) error
	Run()
	WaitMessage(timeout time.Duration) (interface{}, error)
}

type DownStream interface {
	Run(up UpStream, handler func(interface{}) error) error
}

type DownStreamMonitor interface {
	NeedBackOff() bool
}

type SendService interface {
	SendMessage(interface{}, duration time.Duration) (interface{}, error)
}

type SendSpec interface {
	ToSendMessageInput() (*sqs.SendMessageInput, error)
}

// A rate limit.
type RateLimit interface {
	// Wait for $count tokens are granted(return true) or
	// timeout(return false).
	Wait(count int64, timeout time.Duration) bool
}

