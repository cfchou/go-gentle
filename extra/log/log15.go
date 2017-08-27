package log

import (
	"gopkg.in/cfchou/go-gentle.v3/gentle"
	"gopkg.in/inconshreveable/log15.v2"
)

type Log15Adapter struct {
	log15.Logger
}

func NewLog15Adapter(logger log15.Logger) gentle.Logger {
	return &Log15Adapter{
		Logger: logger,
	}
}

func (l *Log15Adapter) New(fields ...interface{}) gentle.Logger {
	return &Log15Adapter{
		Logger: l.Logger.New(fields...),
	}
}
