package gentle

import (
	//"errors"
	//"github.com/afex/hystrix-go/hystrix"
	//"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
	//"github.com/stretchr/testify/mock"
	"strconv"
	"sync"
	"testing"
	"time"
	"errors"
	"github.com/stretchr/testify/mock"
	"github.com/benbjohnson/clock"
	"github.com/rs/xid"
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

// Returns a ChannelStream which contains $count number of mock Messages that
// are also returned.
func genChannelStreamWithMessages(count int) (*ChannelStream, []Message) {
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
		*NewChannelStreamOpts("", "test", src)), msgs
}

func TestChannelStream_Get(t *testing.T) {
	mm := &fakeMsg{id: "123"}
	src := make(chan interface{}, 1)
	src <- mm
	stream := NewChannelStream(
		*NewChannelStreamOpts("","test", src))
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
		*NewChannelStreamOpts("", "test", src))

	stream := NewRateLimitedStream(
		*NewRateLimitedStreamOpts("", "test",
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
	mstream := &mockStream{}

	mback := &mockBackOff{}
	// mock clock so that we don't need to wait for the real timer to move
	// forward
	mclock := clock.NewMock()
	opts := NewRetryStreamOpts("", "test", mback)
	opts.Clock = mclock
	stream := NewRetryStream(*opts, mstream)

	// 1st: ok
	mm := &fakeMsg{id: "123"}
	call := mstream.On("Get")
	call.Return(mm, nil)

	_, err := stream.Get()
	assert.NoError(t, err)

	// 2ed: err, trigger retry with backoffs
	fakeErr := errors.New("A fake error")
	call.Return(nil, fakeErr)

	count := 3
	minimum := time.Duration(count) * time.Second
	mback_next := mback.On("Next")
	mback_next.Run(func(args mock.Arguments) {
		if count == 0 {
			mback_next.Return(BackOffStop)
		} else {
			count--
			mback_next.Return(1 * time.Second)
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
		case dura :=<-timespan:
			log.Info("[Test] spent >= minmum?", "spent", dura, "minimum", minimum)
			assert.True(t, dura >= minimum)
			return
		default:
			// advance an arbitrary time to pass backoff
			mclock.Add(1*time.Second)
		}
	}
}

func TestBulkheadStream_Get(t *testing.T) {
	max_concurrency := 4
	mstream := &mockStream{}

	stream := NewBulkheadStream(
		*NewBulkheadStreamOpts("", "test", max_concurrency),
		mstream)
	mm := &fakeMsg{id: "123"}

	wg := &sync.WaitGroup{}
	wg.Add(max_concurrency)
	block := make(chan struct{}, 0)
	call := mstream.On("Get")
	call.Run(func(args mock.Arguments) {
		wg.Done()
		block <- struct{}{}
	})
	call.Return(mm, nil)

	for i := 0; i < max_concurrency; i++ {
		go stream.Get()
	}

	wg.Wait()
	msg, err := stream.Get()
	assert.Equal(t, msg, nil)
	assert.EqualError(t, err, ErrMaxConcurrency.Error())
	for i := 0; i < max_concurrency; i++ {
		<-block
	}
}

func TestHandlerStream_Get(t *testing.T) {
	mstream := &mockStream{}
	mhandler := &mockHandler{}

	stream := NewHandlerStream(
		*NewHandlerStreamOpts("", "test"),
		mstream, mhandler)
	mm := &fakeMsg{id: "123"}

	get := mstream.On("Get")
	get.Return(mm, nil)

	handle := mhandler.On("Handle", mm)
	handle.Run(func(args mock.Arguments) {
		log.Info("[Test] handle")
		msg := args.Get(0).(*fakeMsg)
		msg.id = "456"
	})
	handle.Return(mm, nil)

	msg, err := stream.Get()
	assert.NoError(t, err)
	assert.Equal(t, msg.Id(), "456")
}

func TestCircuitBreakerStream_Get(t *testing.T) {
	max_concurrency := 4
	circuit := xid.New().String()
	mstream := &mockStream{}

	// requests exceeding MaxConcurrentRequests would get ErrCbMaxConcurrency
	// provided Timeout is large enough for this test case
	conf := NewDefaultCircuitBreakerConf()
	conf.MaxConcurrent = max_concurrency
	conf.Timeout = 10 * time.Second
	conf.RegisterFor(circuit)

	stream := NewCircuitBreakerStream(
		*NewCircuitBreakerStreamOpts("", "test", circuit),
		mstream)
	mm := &fakeMsg{id: "123"}

	var wg sync.WaitGroup
	wg.Add(max_concurrency)
	cond := sync.NewCond(&sync.Mutex{})
	call := mstream.On("Get")
	call.Run(func(args mock.Arguments) {
		// wg.Done() here instead of in the loop guarantees Get() is
		// running by the circuit
		wg.Done()
		cond.L.Lock()
		cond.Wait()
	})
	call.Return(mm, nil)

	for i := 0; i < max_concurrency; i++ {
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
	circuit := xid.New().String()
	mstream := &mockStream{}

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
		*NewCircuitBreakerStreamOpts("", "test", circuit),
		mstream)
	mm := &fakeMsg{id: "123"}

	// Suspend longer than Timeout
	cond := sync.NewCond(&sync.Mutex{})
	suspended := false
	suspend := conf.Timeout + time.Millisecond
	call := mstream.On("Get")
	call.Run(func(args mock.Arguments) {
		cond.L.Lock()
		defer cond.L.Unlock()
		if !suspended {
			// 1st call would suspend for tripping ErrCbTimeout
			suspended = true
			time.Sleep(suspend)
			cond.Broadcast()
		}
	})
	call.Return(mm, nil)

	// 1st call gets ErrCbTimeout
	_, err := stream.Get()
	assert.EqualError(t, err, ErrCbTimeout.Error())

	// Subsequent requests within SleepWindow eventually see ErrCbOpen.
	// "Eventually" because hystrix updates metrics asynchronously.
	//tm := time.NewTimer(IntToMillis(conf.SleepWindow))
	tm := time.NewTimer(conf.SleepWindow)
LOOP:
	for {
		log.Debug("[Test] try again")
		select {
		case <-tm.C:
			assert.Fail(t, "[Test] SleepWindow not long enough")
		default:
			// call Get() many times until circuit becomes open
			_, err := stream.Get()
			if err == ErrCbOpen {
				tm.Stop()
				break LOOP
			}
		}
	}

	// Wait to prevent data race because 1st call might still be running.
	cond.L.Lock()
	for !suspended {
		cond.Wait()
	}
	cond.L.Unlock()

	// After SleepWindow, circuit becomes half-open. Because
	// ErrorPercentThreshold is extremely low so only one successful case
	// is needed to close the circuit.
	time.Sleep(conf.SleepWindow)
	_, err = stream.Get()
	assert.NoError(t, err)
	// In the end, circuit is closed because of no error.
}

func TestCircuitBreakerStream_Get3(t *testing.T) {
	// Test fakeErr and subsequent ErrCbOpen
	circuit := xid.New().String()
	mstream := &mockStream{}

	conf := NewDefaultCircuitBreakerConf()
	// Set RequestVolumeThreshold/ErrorPercentThreshold to be the most
	// sensitive. One request returns error would make the subsequent
	// requests coming within SleepWindow see ErrCbOpen
	conf.VolumeThreshold = 1
	conf.ErrorPercentThreshold = 1
	conf.SleepWindow = time.Second
	conf.RegisterFor(circuit)

	stream := NewCircuitBreakerStream(
		*NewCircuitBreakerStreamOpts("", "test", circuit),
		mstream)
	mm := &fakeMsg{id: "123"}
	fakeErr := errors.New("A fake error")

	call := mstream.On("Get")
	call.Return(nil, fakeErr)

	// 1st call gets fakeErr
	_, err := stream.Get()
	assert.EqualError(t, err, fakeErr.Error())

	// Subsequent requests within SleepWindow eventually see ErrCbOpen.
	// "Eventually" because hystrix updates metrics asynchronously.
	tm := time.NewTimer(conf.SleepWindow)
LOOP:
	for {
		log.Debug("[Test] try again")
		select {
		case <-tm.C:
			assert.Fail(t, "[Test] SleepWindow not long enough")
		default:
			// call Get() many times until circuit becomes open
			_, err := stream.Get()
			if err == ErrCbOpen {
				tm.Stop()
				break LOOP
			}
		}
	}

	// Once circuit is opened, subsequent calls should not run.
	call.Run(func(args mock.Arguments) {
		assert.Fail(t, "[Test] Should not run")
	})
	stream.Get()

	// After SleepWindow, circuit becomes half-open. Only one successful
	// case is needed to close the circuit.
	time.Sleep(conf.SleepWindow)
	call.Run(func(args mock.Arguments) {
		// no-op, for replacing previous one.
	})
	call.Return(mm, nil)
	_, err = stream.Get()
	assert.NoError(t, err)
}

func TestCircuitBreakerStream_Get4(t *testing.T) {
	// Test RequestVolumeThreshold/ErrorPercentThreshold.
	circuit := xid.New().String()
	mstream := &mockStream{}
	countErr := 3
	countSucc := 1
	countRest := 3

	conf := NewDefaultCircuitBreakerConf()
	// Set ErrorPercentThreshold to be the most sensitive(1%). Once
	// RequestVolumeThreshold exceeded, the circuit becomes open.
	conf.VolumeThreshold = countErr + countSucc + countRest
	conf.ErrorPercentThreshold = 1
	conf.SleepWindow = time.Second
	conf.RegisterFor(circuit)

	stream := NewCircuitBreakerStream(
		*NewCircuitBreakerStreamOpts("", "test", circuit),
		mstream)
	fakeErr := errors.New("A fake error")
	mm := &fakeMsg{id: "123"}
	call := mstream.On("Get")

	// countErr is strictly smaller than RequestVolumeThreshold. So circuit
	// is still closed.
	call.Return(nil, fakeErr)
	for i := 0; i < countErr; i++ {
		_, err := stream.Get()
		assert.EqualError(t, err, fakeErr.Error())
	}

	call.Return(mm, nil)
	for i := 0; i < countSucc; i++ {
		// A success on the closed Circuit.
		_, err := stream.Get()
		assert.NoError(t, err)
	}

	// Subsequent requests within SleepWindow eventually see ErrCbOpen.
	// "Eventually" because hystrix updates metrics asynchronously.
	tm := time.NewTimer(conf.SleepWindow)
LOOP:
	for {
		log.Debug("[Test] try again")
		select {
		case <-tm.C:
			assert.Fail(t, "[Test] SleepWindow not long enough")
		default:
			_, err := stream.Get()
			if err == ErrCbOpen {
				tm.Stop()
				break LOOP
			}
		}
	}
}

