// vim:fileencoding=utf-8
package service

import (
	"github.com/inconshreveable/log15"
	"time"
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



