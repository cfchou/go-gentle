package log

import (
	"errors"
	"gopkg.in/cfchou/go-gentle.v3/gentle"
	"gopkg.in/sirupsen/logrus.v1"
)

var (
	errNotEvenFields = errors.New("Number of log fields is not even")
	errFieldType     = errors.New("Not valid log field type")
)

type LogrusLogger struct {
	*logrus.Entry
}

func NewLogusLogger(logger *logrus.Logger) gentle.Logger {
	return &LogrusLogger{
		Entry: logrus.NewEntry(logger),
	}
}

func toFields(fields ...interface{}) (logrus.Fields, error) {
	if len(fields)%2 != 0 {
		return nil, errNotEvenFields
	}
	fs := logrus.Fields{}
	for i := 0; i < len(fields); i += 2 {
		k, ok := fields[i].(string)
		if !ok {
			return nil, errFieldType
		}
		fs[k] = fields[i+1]
	}
	return fs, nil
}

func (l *LogrusLogger) Debug(msg string, fields ...interface{}) {
	if fs, err := toFields(fields...); err != nil {
		l.WithFields(logrus.Fields{
			"origMsg": msg,
		}).Error(err)
	} else {
		l.WithFields(fs).Debug(msg)
	}
}

func (l *LogrusLogger) Info(msg string, fields ...interface{}) {
	if fs, err := toFields(fields...); err != nil {
		l.WithFields(logrus.Fields{
			"origMsg": msg,
		}).Error(err)
	} else {
		l.WithFields(fs).Info(msg)
	}
}

func (l *LogrusLogger) Warn(msg string, fields ...interface{}) {
	if fs, err := toFields(fields...); err != nil {
		l.WithFields(logrus.Fields{
			"origMsg": msg,
		}).Error(err)
	} else {
		l.WithFields(fs).Warn(msg)
	}
}

func (l *LogrusLogger) Error(msg string, fields ...interface{}) {
	if fs, err := toFields(fields...); err != nil {
		l.WithFields(logrus.Fields{
			"origMsg": msg,
		}).Error(err)
	} else {
		l.WithFields(fs).Error(msg)
	}
}

func (l *LogrusLogger) Crit(msg string, fields ...interface{}) {
	if fs, err := toFields(fields...); err != nil {
		l.WithFields(logrus.Fields{
			"origMsg": msg,
		}).Error(err)
	} else {
		l.WithFields(fs).Fatal(msg)
	}
}

func (l *LogrusLogger) New(fields ...interface{}) gentle.Logger {
	if fs, err := toFields(fields...); err != nil {
		l.WithFields(logrus.Fields{
			"err": err,
		}).Error("Logger.New() with invalid fields")
		return &LogrusLogger{
			// a new Entry with no new Fields
			//Entry: logrus.WithFields(logrus.Fields{}),
			Entry: logrus.NewEntry(l.Entry.Logger),
		}
	} else {
		return &LogrusLogger{
			Entry: logrus.NewEntry(l.Entry.Logger).WithFields(fs),
		}
	}
}
