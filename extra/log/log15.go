package log

import (
	"gopkg.in/cfchou/go-gentle.v3/gentle"
	"gopkg.in/inconshreveable/log15.v2"
)

type Log15Logger struct {
	log15.Logger
}

func NewLog15Logger(logger log15.Logger) gentle.Logger {
	return &Log15Logger{
		Logger: logger,
	}
}

func (l *Log15Logger) New(fields ...interface{}) gentle.Logger {
	return &Log15Logger{
		Logger: l.Logger.New(fields...),
	}
}
