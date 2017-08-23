package log

import (
	"gopkg.in/inconshreveable/log15.v2"
	"github.com/cfchou/go-gentle"
)

type Log15Logger struct {
	log15.Logger
}

func (l *Log15Logger) New(fields ...interface{}) go_gentle.Logger {
	return &Log15Logger{
		Logger: l.Logger.New(fields...),
	}
}
