package gentle

import (
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/benbjohnson/clock"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Returns a $src of "chan Message" and $done chan of "chan *struct{}".
// Every Message extracted from $src has a monotonically increasing id.
func genMessageChannelInfinite() (<-chan interface{}, chan *struct{}) {
	done := make(chan *struct{}, 1)
	src := make(chan interface{}, 1)
	go func() {
		count := 0
		for {
			select {
			case <-done:
				log.Info("[Test] Channel closing")
				close(src)
				return
			default:
				count++
				src <- &fakeMsg{id: strconv.Itoa(count)}
			}
		}
	}()
	return src, done
}

// Returns a channelStream which contains $count number of mock Messages that
// are also returned.
func genChannelStreamWithMessages(count int) (*channelStream, []Message) {
	msgs := make([]Message, count)
	for i := 0; i < count; i++ {
		mm := &fakeMsg{id: strconv.Itoa(i)}
		msgs[i] = mm
	}
	src := make(chan interface{}, 1)
	go func() {
		for i := 0; i < count; i++ {
			src <- msgs[i]
		}
	}()
	return NewChannelStream(
		NewChannelStreamOpts("", "test", src)), msgs
}

func TestChannelStream_Get(t *testing.T) {
	mm := &fakeMsg{id: "123"}
	src := make(chan interface{}, 1)
	src <- mm
	stream := NewChannelStream(
		NewChannelStreamOpts("", "test", src))
	msg_out, err := stream.Get()
	assert.NoError(t, err)
	assert.Equal(t, msg_out.Id(), mm.Id())
}

func TestChannelStream_Get_2(t *testing.T) {
	count := 10
	stream, msgs := genChannelStreamWithMessages(count)

	for i := 0; i < count; i++ {
		msg_out, err := stream.Get()
		assert.NoError(t, err)
		assert.Equal(t, msg_out.Id(), msgs[i].Id())
	}
}

func TestRateLimitedStream_Get(t *testing.T) {
	src, done := genMessageChannelInfinite()
	// 1 msg/sec
	requests_interval := 100 * time.Millisecond

	chanStream := NewChannelStream(
		NewChannelStreamOpts("", "test", src))

	stream := NewRateLimitedStream(
		NewRateLimitedStreamOpts("", "test",
			NewTokenBucketRateLimit(requests_interval, 1)),
		chanStream)
	count := 4
	minimum := time.Duration(count-1) * requests_interval
	var wg sync.WaitGroup
	wg.Add(count)
	begin := time.Now()
	for i := 0; i < count; i++ {
		go func() {
			_, err := stream.Get()
			assert.NoError(t, err)
			wg.Done()
		}()
	}
	wg.Wait()
	dura := time.Now().Sub(begin)
	log.Info("[Test] spent >= minmum?", "spent", dura, "minimum", minimum)
	assert.True(t, dura >= minimum)
	done <- &struct{}{}
}

func TestRetryStream_Get(t *testing.T) {
	// Test against mocked BackOff
	mfactory := &MockBackOffFactory{}
	mback := &MockBackOff{}
	// mock clock so that we don't need to wait for the real timer to move
	// forward
	mclock := clock.NewMock()
	opts := NewRetryStreamOpts("", "test", mfactory)
	opts.Clock = mclock
	mstream := &MockStream{}
	stream := NewRetryStream(opts, mstream)

	// 1st: ok
	mm := &fakeMsg{id: "123"}
	call := mstream.On("Get")
	call.Return(mm, nil)
	_, err := stream.Get()
	assert.NoError(t, err)

	// 2ed: err, trigger retry with backoffs
	fakeErr := errors.New("A fake error")
	call.Return((*fakeMsg)(nil), fakeErr)
	// create a backoff that fires 1 second for $count times
	count := 3
	timespan_minimum := time.Duration(count) * time.Second
	mfactory.On("NewBackOff").Return(mback)
	mback.On("Next").Return(func() time.Duration {
		if count == 0 {
			return BackOffStop
		} else {
			count--
			return 1 * time.Second
		}
	})
	timespan := make(chan time.Duration, 1)
	go func() {
		begin := mclock.Now()
		_, err = stream.Get()
		// backoffs exhausted
		assert.EqualError(t, err, fakeErr.Error())
		timespan <- mclock.Now().Sub(begin)
	}()

	for {
		select {
		case dura := <-timespan:
			log.Info("[Test] spent >= minmum?", "spent", dura, "timespan_minimum", timespan_minimum)
			assert.True(t, dura >= timespan_minimum)
			return
		default:
			// advance an arbitrary time to pass all backoffs
			mclock.Add(1 * time.Second)
		}
	}
}

func TestRetryStream_Get2(t *testing.T) {
	// Test against ConstantBackOff
	mclock := clock.NewMock()
	timespan_minimum := 16 * time.Second
	backOffOpts := NewConstantBackOffFactoryOpts(time.Second, timespan_minimum)
	backOffOpts.Clock = mclock
	backOffFactory := NewConstantBackOffFactory(backOffOpts)
	opts := NewRetryStreamOpts("", "test", backOffFactory)
	opts.Clock = mclock
	mstream := &MockStream{}
	stream := NewRetryStream(opts, mstream)

	// 1st: ok
	mm := &fakeMsg{id: "123"}
	call := mstream.On("Get")
	call.Return(mm, nil)
	_, err := stream.Get()
	assert.NoError(t, err)

	// 2ed: err, trigger retry with backoffs
	fakeErr := errors.New("A fake error")
	call.Return((*fakeMsg)(nil), fakeErr)

	timespan := make(chan time.Duration, 1)
	go func() {
		begin := mclock.Now()
		_, err = stream.Get()
		// backoffs exhausted
		assert.EqualError(t, err, fakeErr.Error())
		timespan <- mclock.Now().Sub(begin)
	}()

	for {
		select {
		case dura := <-timespan:
			log.Info("[Test] spent >= minmum?", "spent", dura, "timespan_minimum", timespan_minimum)
			assert.True(t, dura >= timespan_minimum)
			return
		default:
			// advance an arbitrary time to pass all backoffs
			mclock.Add(1 * time.Second)
		}
	}
}

func TestRetryStream_Get3(t *testing.T) {
	// Test against ExponentialBackOff
	mclock := clock.NewMock()
	timespan_minimum := 1024 * time.Second
	backOffOpts := NewExponentialBackOffFactoryOpts(time.Second, 2.0,
		256*time.Second, timespan_minimum)
	// No randomization to make the growth of backoff time approximately
	// exponential.
	backOffOpts.RandomizationFactor = 0
	backOffOpts.Clock = mclock
	backOffFactory := NewExponentialBackOffFactory(backOffOpts)
	opts := NewRetryStreamOpts("", "test", backOffFactory)
	opts.Clock = mclock
	mstream := &MockStream{}
	stream := NewRetryStream(opts, mstream)

	// 1st: ok
	mm := &fakeMsg{id: "123"}
	call := mstream.On("Get")
	call.Return(mm, nil)
	_, err := stream.Get()
	assert.NoError(t, err)

	// 2ed: err, trigger retry with backoffs
	fakeErr := errors.New("A fake error")
	call.Return((*fakeMsg)(nil), fakeErr)

	timespan := make(chan time.Duration, 1)
	go func() {
		begin := mclock.Now()
		_, err = stream.Get()
		// backoffs exhausted
		assert.EqualError(t, err, fakeErr.Error())
		timespan <- mclock.Now().Sub(begin)
	}()

	for {
		select {
		case dura := <-timespan:
			log.Info("[Test] spent >= minmum?", "spent", dura, "timespan_minimum", timespan_minimum)
			assert.True(t, dura >= timespan_minimum)
			return
		default:
			// advance an arbitrary time to pass all backoffs
			mclock.Add(1 * time.Second)
		}
	}
}

func TestRetryStream_Get4(t *testing.T) {
	// Test against concurrent retryStream.Get() and ConstantBackOff
	mclock := clock.NewMock()
	timespan_minimum := 16 * time.Second
	backOffOpts := NewConstantBackOffFactoryOpts(time.Second, timespan_minimum)
	backOffOpts.Clock = mclock
	backOffFactory := NewConstantBackOffFactory(backOffOpts)
	opts := NewRetryStreamOpts("", "test", backOffFactory)
	opts.Clock = mclock
	mstream := &MockStream{}
	stream := NewRetryStream(opts, mstream)

	// 1st: ok
	mm := &fakeMsg{id: "123"}
	call := mstream.On("Get")
	call.Return(mm, nil)
	_, err := stream.Get()
	assert.NoError(t, err)

	// 2ed: err, trigger retry with backoffs
	fakeErr := errors.New("A fake error")
	call.Return((*fakeMsg)(nil), fakeErr)

	count := 2
	wg := sync.WaitGroup{}
	wg.Add(count)
	done := make(chan struct{}, 1)

	for i := 0; i < count; i++ {
		go func() {
			begin := mclock.Now()
			_, err = stream.Get()
			// backoffs exhausted
			assert.EqualError(t, err, fakeErr.Error())
			dura := mclock.Now().Sub(begin)
			log.Info("[Test] spent >= minmum?", "spent", dura, "timespan_minimum", timespan_minimum)
			assert.True(t, dura >= timespan_minimum)
			wg.Done()
		}()

	}
	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	begin := mclock.Now()
	for {
		select {
		case <-done:
			log.Info("[Test] all done", "spent", mclock.Now().Sub(begin))
			return
		default:
			// advance an arbitrary time to pass all backoffs
			mclock.Add(1 * time.Second)
		}
	}
}

func TestRetryStream_Get5(t *testing.T) {
	// Test against concurrent retryStream.Get() and ExponentialBackOff
	mclock := clock.NewMock()
	timespan_minimum := 1024 * time.Second
	backOffOpts := NewExponentialBackOffFactoryOpts(time.Second, 2.0,
		256*time.Second, timespan_minimum)
	// No randomization to make the growth of backoff time approximately
	// exponential.
	backOffOpts.RandomizationFactor = 0
	backOffOpts.Clock = mclock
	backOffFactory := NewExponentialBackOffFactory(backOffOpts)
	opts := NewRetryStreamOpts("", "test", backOffFactory)
	opts.Clock = mclock
	mstream := &MockStream{}
	stream := NewRetryStream(opts, mstream)

	// 1st: ok
	mm := &fakeMsg{id: "123"}
	call := mstream.On("Get")
	call.Return(mm, nil)
	_, err := stream.Get()
	assert.NoError(t, err)

	// 2ed: err, trigger retry with backoffs
	fakeErr := errors.New("A fake error")
	call.Return((*fakeMsg)(nil), fakeErr)

	count := 2
	wg := sync.WaitGroup{}
	wg.Add(count)
	done := make(chan struct{}, 1)

	for i := 0; i < count; i++ {
		go func() {
			begin := mclock.Now()
			_, err = stream.Get()
			// backoffs exhausted
			assert.EqualError(t, err, fakeErr.Error())
			dura := mclock.Now().Sub(begin)
			log.Info("[Test] spent >= minmum?", "spent", dura, "timespan_minimum", timespan_minimum)
			assert.True(t, dura >= timespan_minimum)
			wg.Done()
		}()

	}
	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	begin := mclock.Now()
	for {
		select {
		case <-done:
			log.Info("[Test] all done", "spent", mclock.Now().Sub(begin))
			return
		default:
			// advance an arbitrary time to pass all backoffs
			mclock.Add(1 * time.Second)
		}
	}
}

func TestBulkheadStream_Get(t *testing.T) {
	maxConcurrency := 4
	mstream := &MockStream{}
	stream := NewBulkheadStream(
		NewBulkheadStreamOpts("", "test", maxConcurrency),
		mstream)
	mm := &fakeMsg{id: "123"}

	wg := &sync.WaitGroup{}
	wg.Add(maxConcurrency)
	block := make(chan struct{}, 0)
	mstream.On("Get").Return(
		func() Message {
			wg.Done()
			// every Get() would be blocked here
			block <- struct{}{}
			return mm
		}, nil)

	for i := 0; i < maxConcurrency; i++ {
		go stream.Get()
	}

	// Wait() until $maxConcurrency of Get() are blocked
	wg.Wait()
	// one more Get() would cause ErrMaxConcurrency
	msg, err := stream.Get()
	assert.Equal(t, msg, nil)
	assert.EqualError(t, err, ErrMaxConcurrency.Error())
	// Release blocked
	for i := 0; i < maxConcurrency; i++ {
		<-block
	}
}

func TestSemaphoreStream_Get(t *testing.T) {
	maxConcurrency := 4
	mstream := &MockStream{}
	stream := NewSemaphoreStream(
		NewSemaphoreStreamOpts("", "test", maxConcurrency),
		mstream)
	mm := &fakeMsg{id: "123"}

	wg := &sync.WaitGroup{}
	wg.Add(maxConcurrency)
	block := make(chan struct{}, 0)
	mstream.On("Get").Return(
		func() Message {
			wg.Done()
			// every Get() would be blocked here
			block <- struct{}{}
			return mm
		}, nil)

	for i := 0; i < maxConcurrency; i++ {
		go stream.Get()
	}
	// Wait() until $maxConcurrency of Get() are blocked
	wg.Wait()

	go func() {
		// blocked...
		stream.Get()
		assert.Fail(t, "Get() should be blocked")
	}()
	// TODO:
	// Sleep to see if assert.Fail() is triggered. This isn't perfect but it's
	// not easy to prove the previous stream.Get() is blocked forever.
	time.Sleep(2 * time.Second)
}

func TestHandlerMappedStream_Get(t *testing.T) {
	mstream := &MockStream{}
	mhandler := &MockHandler{}
	stream := NewHandlerMappedStream(
		NewHandlerMappedStreamOpts("", "test"),
		mstream, mhandler)
	mm := &fakeMsg{id: "123"}

	get := mstream.On("Get")
	get.Return(mm, nil)

	newMM := &fakeMsg{id: "456"}
	mhandler.On("Handle", mm).Return(
		func(msg Message) Message {
			return newMM
		}, nil)

	msg, err := stream.Get()
	assert.NoError(t, err)
	assert.Equal(t, newMM.Id(), msg.Id())
}

func TestCircuitBreakerStream_Get(t *testing.T) {
	defer hystrix.Flush()
	maxConcurrency := 4
	circuit := xid.New().String()
	mstream := &MockStream{}

	// requests exceeding MaxConcurrentRequests would get
	// ErrCbMaxConcurrency provided that Timeout is large enough for this
	// test case
	conf := NewDefaultCircuitBreakerConf()
	conf.MaxConcurrent = maxConcurrency
	conf.Timeout = 10 * time.Second
	conf.RegisterFor(circuit)

	stream := NewCircuitBreakerStream(
		NewCircuitBreakerStreamOpts("", "test", circuit),
		mstream)
	mm := &fakeMsg{id: "123"}

	var wg sync.WaitGroup
	wg.Add(maxConcurrency)
	cond := sync.NewCond(&sync.Mutex{})
	mstream.On("Get").Return(func() Message {
		// wg.Done() here instead of in the loop guarantees Get() is
		// running by the circuit
		wg.Done()
		cond.L.Lock()
		cond.Wait()
		return mm
	}, nil)

	for i := 0; i < maxConcurrency; i++ {
		go func() {
			stream.Get()
		}()
	}
	// Make sure previous Get() are all running before the next
	wg.Wait()
	// One more call while all previous calls sticking in the circuit
	_, err := stream.Get()
	assert.EqualError(t, err, ErrCbMaxConcurrency.Error())
	cond.Broadcast()
}

func TestCircuitBreakerStream_Get2(t *testing.T) {
	// Test ErrCbTimeout and subsequent ErrCbOpen
	defer hystrix.Flush()
	circuit := xid.New().String()
	mstream := &MockStream{}

	conf := NewDefaultCircuitBreakerConf()
	// Set RequestVolumeThreshold/ErrorPercentThreshold to be the most
	// sensitive. One request hits timeout would make the subsequent
	// requests coming within SleepWindow see ErrCbOpen
	conf.VolumeThreshold = 1
	conf.ErrorPercentThreshold = 1
	conf.SleepWindow = time.Second
	// A short Timeout to speed up the test
	conf.Timeout = time.Millisecond
	conf.RegisterFor(circuit)

	stream := NewCircuitBreakerStream(
		NewCircuitBreakerStreamOpts("", "test", circuit),
		mstream)
	var nth int64
	newMsg := func() Message {
		tmp := atomic.AddInt64(&nth, 1)
		return &fakeMsg{id: strconv.FormatInt(tmp, 10)}
	}

	// Suspend longer than Timeout
	var to_suspend int64
	suspend := conf.Timeout + time.Millisecond
	mstream.On("Get").Return(
		func() Message {
			if atomic.LoadInt64(&to_suspend) == 0 {
				time.Sleep(suspend)
			}
			return newMsg()
		}, nil)

	// ErrCbTimeout then the subsequent requests within SleepWindow eventually
	// see ErrCbOpen "Eventually" because hystrix updates metrics asynchronously.
	for {
		log.Debug("[Test] try again for ErrCbOpen")
		_, err := stream.Get()
		if err == ErrCbOpen {
			break
		}
		// err could be nil if $suspend is short and scheduler is slow.
		// if that's the case, we'll try until threshold is reached.
	}

	// Disable to_suspend
	atomic.StoreInt64(&to_suspend, 1)
	for {
		time.Sleep(conf.SleepWindow)
		log.Debug("[Test] try again for no err")
		_, err := stream.Get()
		if err == nil {
			// In the end, circuit is closed because of no error.
			break
		}
		// err could be ErrCbOpen or even ErrCbTimeout if scheduling
		// is slow. If that's the case, we'll try until circuit is closed.
	}
}

func TestCircuitBreakerStream_Get3(t *testing.T) {
	// Test fakeErr and subsequent ErrCbOpen
	defer hystrix.Flush()
	circuit := xid.New().String()
	mstream := &MockStream{}

	conf := NewDefaultCircuitBreakerConf()
	// Set RequestVolumeThreshold/ErrorPercentThreshold to be the most
	// sensitive. One request returns error would make the subsequent
	// requests coming within SleepWindow see ErrCbOpen
	conf.VolumeThreshold = 1
	conf.ErrorPercentThreshold = 1
	conf.SleepWindow = time.Second
	conf.RegisterFor(circuit)

	stream := NewCircuitBreakerStream(
		NewCircuitBreakerStreamOpts("", "test", circuit),
		mstream)
	mm := &fakeMsg{id: "123"}
	fakeErr := errors.New("A fake error")

	call := mstream.On("Get")
	call.Return((*fakeMsg)(nil), fakeErr)

	// 1st call gets fakeErr
	_, err := stream.Get()
	assert.EqualError(t, err, fakeErr.Error())

	// Subsequent requests within SleepWindow eventually see ErrCbOpen.
	// "Eventually" because hystrix updates metrics asynchronously.
	for {
		log.Debug("[Test] try again")
		_, err := stream.Get()
		if err == ErrCbOpen {
			break
		} else {
			assert.EqualError(t, err, fakeErr.Error())
		}
	}

	call.Return(mm, nil)
	for {
		time.Sleep(conf.SleepWindow)
		log.Debug("[Test] try again for no err")
		_, err := stream.Get()
		if err == nil {
			// In the end, circuit is closed because of no error.
			break
		} else {
			assert.EqualError(t, err, ErrCbOpen.Error())
		}
	}
}

func TestFallbackStream_Get(t *testing.T) {
	// fallBackFunc is not called when no error
	mm := &fakeMsg{id: "123"}
	fallBackFunc := func(err error) (Message, error) {
		assert.Fail(t, "Shouldn't trigger fallback")
		return nil, err
	}
	mstream := &MockStream{}
	fstream := NewFallbackStream(
		NewFallbackStreamOpts("", "test", fallBackFunc),
		mstream)
	mstream.On("Get").Return(mm, nil)

	msg, err := fstream.Get()
	assert.NoError(t, err)
	assert.Equal(t, msg.Id(), mm.Id())
}

func TestFallbackStream_Get2(t *testing.T) {
	// fallBackFunc is called when error
	fakeErr := errors.New("A fake error")
	fallbackCalled := false
	fallbackFunc := func(err error) (Message, error) {
		assert.EqualError(t, err, fakeErr.Error())
		fallbackCalled = true
		return nil, err
	}
	mstream := &MockStream{}
	fstream := NewFallbackStream(
		NewFallbackStreamOpts("", "test", fallbackFunc),
		mstream)
	mstream.On("Get").Return((*fakeMsg)(nil), fakeErr)

	msg, err := fstream.Get()
	assert.Nil(t, msg)
	assert.EqualError(t, err, fakeErr.Error())
	assert.True(t, fallbackCalled)
}

func TestFallbackStream_Get3(t *testing.T) {
	// fallBackFunc is called when error, it can replace the error with a msg.
	mm := &fakeMsg{id: "123"}
	fakeErr := errors.New("A fake error")
	fallbackFunc := func(err error) (Message, error) {
		assert.EqualError(t, err, fakeErr.Error())
		return mm, nil
	}
	mstream := &MockStream{}
	fstream := NewFallbackStream(
		NewFallbackStreamOpts("", "test", fallbackFunc),
		mstream)
	mstream.On("Get").Return((*fakeMsg)(nil), fakeErr)

	msg, err := fstream.Get()
	assert.NoError(t, err)
	assert.Equal(t, msg.Id(), mm.Id())
}
