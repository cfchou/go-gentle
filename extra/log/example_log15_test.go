package log

import (
	"context"
	"gopkg.in/cfchou/go-gentle.v3/gentle"
	"gopkg.in/inconshreveable/log15.v2"
	"strconv"
	"time"
)

func ExampleNewLog15Logger_package() {
	// set up log level for package-global logger
	logger := log15.New()
	h := log15.LvlFilterHandler(log15.LvlDebug, log15.StdoutHandler)
	logger.SetHandler(h)
	gentle.Log = NewLog15Logger(logger)

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
	// t=2017-08-23T23:50:50+0800 lvl=info msg="[Stream] Get(), no span" namespace= gentle=sRate name=test err="No parent span"
	// t=2017-08-23T23:50:50+0800 lvl=dbug msg="[Stream] Get() ok" namespace= gentle=sRate name=test msgOut=0 timespan=0.000
	stream.Get(context.Background())
	// Output:
}

func ExampleNewLog15Logger_stream() {
	// set up log level for stream logger
	msgID := 0
	var fakeStream gentle.SimpleStream = func(_ context.Context) (gentle.Message, error) {
		defer func() { msgID++ }()
		return gentle.SimpleMessage(strconv.Itoa(msgID)), nil
	}

	limiter := gentle.NewTokenBucketRateLimit(10*time.Millisecond, 1)

	opts := gentle.NewRateLimitedStreamOpts("", "test2", limiter)
	ll := log15.New()
	ll.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, log15.StdoutHandler))
	opts.Log = NewLog15Logger(ll)
	stream := gentle.NewRateLimitedStream(opts, fakeStream)

	// log:
	// t=2017-08-23T23:54:46+0800 lvl=info msg="[Stream] Get(), no span" err="No parent span"
	// t=2017-08-23T23:54:46+0800 lvl=dbug msg="[Stream] Get() ok" msgOut=0 timespan=0.000
	stream.Get(context.Background())
	// Output:
}
