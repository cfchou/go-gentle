package log

import (
	"context"
	"gopkg.in/cfchou/go-gentle.v3/gentle"
	"gopkg.in/sirupsen/logrus.v1"
	"strconv"
	"time"
)

func ExampleNewLogusLogger_package() {
	// Demo how to replace package-global logger with logrus logger
	ll := logrus.New()
	ll.SetLevel(logrus.InfoLevel)
	// replace package-global logger
	gentle.Log = NewLogusLogger(ll)

	msgID := 0
	var fakeStream gentle.SimpleStream = func(_ context.Context) (gentle.Message, error) {
		defer func() { msgID++ }()
		return gentle.SimpleMessage(strconv.Itoa(msgID)), nil
	}

	limiter := gentle.NewTokenBucketRateLimit(10*time.Millisecond, 1)
	stream := gentle.NewRateLimitedStream(
		gentle.NewRateLimitedStreamOpts("", "test", limiter),
		fakeStream)
	// log:
	// time="2017-08-23T23:56:14+08:00" level=info msg="[Stream] Get(), no span" err="No parent span" gentle=sRate name=test namespace=
	stream.Get(context.Background())
	// Output:
}

func ExampleNewLogusLogger_stream() {
	msgID := 0
	var fakeStream gentle.SimpleStream = func(_ context.Context) (gentle.Message, error) {
		defer func() { msgID++ }()
		return gentle.SimpleMessage(strconv.Itoa(msgID)), nil
	}

	limiter := gentle.NewTokenBucketRateLimit(10*time.Millisecond, 1)
	opts := gentle.NewRateLimitedStreamOpts("", "test2", limiter)
	// replace stream2's logger with logrus one and set its level to debug
	ll := logrus.New()
	ll.SetLevel(logrus.DebugLevel)
	opts.Log = NewLogusLogger(ll)
	stream := gentle.NewRateLimitedStream(opts, fakeStream)

	// log:
	// time="2017-08-23T23:57:11+08:00" level=info msg="[Stream] Get(), no span" err="No parent span"
	// time="2017-08-23T23:57:11+08:00" level=debug msg="[Stream] Get() ok" msgOut=0 timespan=2.6865e-05
	stream.Get(context.Background())
	// Output:
}
