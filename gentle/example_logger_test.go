package gentle

import (
	"context"
	"gopkg.in/inconshreveable/log15.v2"
	"gopkg.in/sirupsen/logrus.v1"
	"strconv"
	"time"
)

func Example_log15_set_level_package() {
	// set up log level for package-global logger
	logger := log15.New()
	h := log15.LvlFilterHandler(log15.LvlDebug, log15.StdoutHandler)
	logger.SetHandler(h)
	Log = &log15Logger{Logger: logger}

	msgId := 0
	var fakeStream SimpleStream = func(_ context.Context) (Message, error) {
		defer func() { msgId++ }()
		return SimpleMessage(strconv.Itoa(msgId)), nil
	}

	limiter := NewTokenBucketRateLimit(10*time.Millisecond, 1)
	stream := NewRateLimitedStream(
		NewRateLimitedStreamOpts("", "test", limiter),
		fakeStream)

	stream.Get(context.Background())
	// Output:
}

func Example_log15_set_level_stream() {
	// set up log level for stream logger
	msgId := 0
	var fakeStream SimpleStream = func(_ context.Context) (Message, error) {
		defer func() { msgId++ }()
		return SimpleMessage(strconv.Itoa(msgId)), nil
	}

	limiter := NewTokenBucketRateLimit(10*time.Millisecond, 1)
	stream := NewRateLimitedStream(
		NewRateLimitedStreamOpts("", "test", limiter),
		fakeStream)

	opts := NewRateLimitedStreamOpts("", "test2", limiter)
	ll := log15.New()
	ll.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, log15.StdoutHandler))
	opts.Log = &log15Logger{Logger: ll}
	stream2 := NewRateLimitedStream(opts, fakeStream)

	// Default level is info:
	// t=2017-08-22T16:48:09+0800 lvl=info msg="[Stream] Get(), no span" namespace= gentle=sRate name=test err="No parent span"
	stream.Get(context.Background())
	// level is debug:
	// t=2017-08-22T16:48:09+0800 lvl=info msg="[Stream] Get(), no span" err="No parent span"
	// t=2017-08-22T16:48:09+0800 lvl=dbug msg="[Stream] Get() ok" msgOut=1 timespan=0.012
	stream2.Get(context.Background())
	// Output:
}

type logrusLogger struct {
	*logrus.Entry
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

func (l *logrusLogger) Debug(msg string, fields ...interface{}) {
	if fs, err := toFields(fields...); err != nil {
		l.WithFields(logrus.Fields{
			"origMsg": msg,
		}).Error(err)
	} else {
		l.WithFields(fs).Debug(msg)
	}
}

func (l *logrusLogger) Info(msg string, fields ...interface{}) {
	if fs, err := toFields(fields...); err != nil {
		l.WithFields(logrus.Fields{
			"origMsg": msg,
		}).Error(err)
	} else {
		l.WithFields(fs).Info(msg)
	}
}

func (l *logrusLogger) Warn(msg string, fields ...interface{}) {
	if fs, err := toFields(fields...); err != nil {
		l.WithFields(logrus.Fields{
			"origMsg": msg,
		}).Error(err)
	} else {
		l.WithFields(fs).Warn(msg)
	}
}

func (l *logrusLogger) Error(msg string, fields ...interface{}) {
	if fs, err := toFields(fields...); err != nil {
		l.WithFields(logrus.Fields{
			"origMsg": msg,
		}).Error(err)
	} else {
		l.WithFields(fs).Error(msg)
	}
}

func (l *logrusLogger) Crit(msg string, fields ...interface{}) {
	if fs, err := toFields(fields...); err != nil {
		l.WithFields(logrus.Fields{
			"origMsg": msg,
		}).Error(err)
	} else {
		l.WithFields(fs).Fatal(msg)
	}
}

func (l *logrusLogger) New(fields ...interface{}) Logger {
	if fs, err := toFields(fields...); err != nil {
		l.WithFields(logrus.Fields{
			"err": err,
		}).Error("Logger.New() with invalid fields")
		return &logrusLogger{
			// a new Entry with no new Fields
			//Entry: logrus.WithFields(logrus.Fields{}),
			Entry: logrus.NewEntry(l.Entry.Logger),
		}
	} else {
		return &logrusLogger{
			Entry: logrus.NewEntry(l.Entry.Logger).WithFields(fs),
		}
	}
}

func Example_logrus_replace_package_logger() {
	// Demo how to replace package-global logger with logrus logger
	ll := logrus.New()
	ll.SetLevel(logrus.InfoLevel)
	// replace package-global logger
	Log = &logrusLogger{
		Entry: logrus.NewEntry(ll),
	}

	msgId := 0
	var fakeStream SimpleStream = func(_ context.Context) (Message, error) {
		defer func() { msgId++ }()
		return SimpleMessage(strconv.Itoa(msgId)), nil
	}

	limiter := NewTokenBucketRateLimit(10*time.Millisecond, 1)
	stream := NewRateLimitedStream(
		NewRateLimitedStreamOpts("", "test", limiter),
		fakeStream)
	stream.Get(context.Background())
	// Output:
}

func Example_logrus_replace_stream_logger() {
	msgId := 0
	var fakeStream SimpleStream = func(_ context.Context) (Message, error) {
		defer func() { msgId++ }()
		return SimpleMessage(strconv.Itoa(msgId)), nil
	}

	limiter := NewTokenBucketRateLimit(10*time.Millisecond, 1)
	stream := NewRateLimitedStream(
		NewRateLimitedStreamOpts("", "test", limiter),
		fakeStream)

	opts := NewRateLimitedStreamOpts("", "test2", limiter)
	// replace stream2's logger with logrus one and set its level to debug
	ll := logrus.New()
	ll.SetLevel(logrus.DebugLevel)
	opts.Log = &logrusLogger{
		Entry: logrus.NewEntry(ll),
	}
	stream2 := NewRateLimitedStream(opts, fakeStream)

	// log15(default) output:
	// t=2017-08-22T16:08:13+0800 lvl=info msg="[Stream] Get(), no span" namespace= gentle=sRate name=test err="No parent span"
	stream.Get(context.Background())
	// logrus output:
	// time="2017-08-22T16:08:13+08:00" level=info msg="[Stream] Get(), no span" err="No parent span"
	// time="2017-08-22T16:15:04+08:00" level=debug msg="[Stream] Get() ok" msgOut=1 timespan=0.010294057
	stream2.Get(context.Background())
	// Output:
}
