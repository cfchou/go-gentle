package gentle

import (
	"errors"
	"fmt"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Returns a $src of "chan Message" and $done chan of "chan *struct{}".
// Every Message extracted from $src has a monotonically increasing id.
func genMessageChannelInfinite() (<-chan Message, chan *struct{}) {
	done := make(chan *struct{}, 1)
	src := make(chan Message, 1)
	go func() {
		count := 1
		for {
			mm := &mockMsg{}
			mm.On("Id").Return(fmt.Sprint(count))
			select {
			case src <- mm:
			case <-done:
				log.Info("[Test] Channel closed")
				break
			}
		}
		close(src)
	}()
	return src, done
}

// Returns a ChannelStream which contains $count number of mock Messages that
// are also returned.
func genChannelStreamWithMessages(count int) (*ChannelStream, []Message) {
	msgs := make([]Message, count)
	for i := 0; i < count; i++ {
		mm := &mockMsg{}
		mm.On("Id").Return(fmt.Sprint(i))
		msgs[i] = mm
	}
	src := make(chan Message, 1)
	go func() {
		for i := 0; i < count; i++ {
			src <- msgs[i]
		}
	}()
	return NewChannelStream("", "test", src), msgs
}

func TestChannelStream_Get(t *testing.T) {
	mm := &mockMsg{}
	mm.On("Id").Return("123")
	src := make(chan Message, 1)
	src <- mm
	stream := NewChannelStream("","test", src)
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
	requests_interval := 1000
	stream := NewRateLimitedStream("", "test",
		NewChannelStream("", "test", src),
		NewTokenBucketRateLimit(requests_interval, 1))
	count := 4
	minimum := time.Duration((count-1)*requests_interval) * time.Millisecond
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
	backoffs := []time.Duration{1 * time.Second, 2 * time.Second}
	minimum := func(backoffs []time.Duration) time.Duration {
		dura_sum := 0 * time.Second
		for _, dura := range backoffs {
			dura_sum += dura
		}
		return dura_sum
	}(backoffs)
	stream := NewRetryStream("", "test", mstream, backoffs)

	// 1st: ok
	mm := &mockMsg{}
	mm.On("Id").Return("123")
	call := mstream.On("Get")
	call.Return(mm, nil)

	_, err := stream.Get()
	assert.NoError(t, err)

	// 2ed: err, trigger retry with backoffs
	mockErr := errors.New("A mocked error")
	call.Return(nil, mockErr)

	begin := time.Now()
	_, err = stream.Get()
	dura := time.Now().Sub(begin)
	// backoffs exhausted
	assert.EqualError(t, err, mockErr.Error())
	log.Info("[Test] spent >= minmum?", "spent", dura, "minimum", minimum)
	assert.True(t, dura >= minimum)
}

func TestBulkheadStream_Get(t *testing.T) {
	count := 8
	max_concurrency := 4
	mstream := &mockStream{}
	stream := NewBulkheadStream("", "test", mstream, max_concurrency)
	mm := &mockMsg{}
	mm.On("Id").Return("123")

	suspend := 100 * time.Millisecond
	lock := &sync.RWMutex{}
	calling := 0
	callings := []int{}
	call := mstream.On("Get")
	call.Run(func(args mock.Arguments) {
		// add calling
		lock.Lock()
		calling++
		callings = append(callings, calling)
		lock.Unlock()
		time.Sleep(suspend)
		// release calling
		lock.Lock()
		calling--
		lock.Unlock()
	})
	call.Return(mm, nil)

	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			msg, err := stream.Get()
			wg.Done()
			assert.NoError(t, err)
			assert.Equal(t, msg.Id(), mm.Id())
		}()
	}
	wg.Wait()
	assert.True(t, count == len(callings))
	for _, c := range callings {
		assert.True(t, c <= max_concurrency)
	}
}

func TestMappedStream_Get(t *testing.T) {
	mstream := &mockStream{}
	mhandler := &mockHandler{}
	mm := &mockMsg{}

	stream := NewMappedStream("", "test", mstream, mhandler)

	call := mm.On("Id")
	call.Return("123")
	receive := mstream.On("Get")
	receive.Return(mm, nil)
	handle := mhandler.On("Handle", mm)
	handle.Run(func(args mock.Arguments) {
		log.Info("[Test] handle")
		call.Return("456")
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

	// requests exceeding MaxConcurrentRequests would get ErrMaxConcurrency
	// provided Timeout is large enough for this test case
	conf := GetHystrixDefaultConfig()
	conf.MaxConcurrentRequests = max_concurrency
	conf.Timeout = 10000
	hystrix.ConfigureCommand(circuit, *conf)

	stream := NewCircuitBreakerStream("","test", mstream, circuit)
	mm := &mockMsg{}
	mm.On("Id").Return("123")

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
	assert.EqualError(t, err, ErrMaxConcurrency.Error())
	cond.Broadcast()
}

func TestCircuitBreakerStream_Get2(t *testing.T) {
	// Test ErrTimeout and subsequent ErrCircuitOpen
	circuit := xid.New().String()
	mstream := &mockStream{}
	count := 3

	conf := GetHystrixDefaultConfig()
	// Set RequestVolumeThreshold/ErrorPercentThreshold to be the most
	// sensitive. One request hits timeout would make the subsequent
	// requests coming within SleepWindow see ErrCircuitOpen
	conf.RequestVolumeThreshold = 1
	conf.ErrorPercentThreshold = 1
	conf.SleepWindow = 1000
	conf.Timeout = 1000
	hystrix.ConfigureCommand(circuit, *conf)

	stream := NewCircuitBreakerStream("","test", mstream, circuit)

	mm := &mockMsg{}
	mm.On("Id").Return("123")

	// Suspend longer than Timeout
	suspend := time.Duration(conf.Timeout+500) * time.Millisecond
	call := mstream.On("Get")
	call.Run(func(args mock.Arguments) {
		time.Sleep(suspend)
	})
	call.Return(mm, nil)

	// 1st call gets ErrTimeout which make subsequent requests within
	// SleepWindow see ErrCircuitOpen
	_, err := stream.Get()
	assert.EqualError(t, err, ErrTimeout.Error())

	var called int64
	call.Run(func(args mock.Arguments) {
		atomic.StoreInt64(&called, 1)
	})
	for i := 0; i < count; i++ {
		_, err := stream.Get()
		assert.EqualError(t, err, ErrCircuitOpen.Error())
	}
	// ErrCircuitOpen prevents Get() from execution.
	assert.Equal(t, atomic.LoadInt64(&called), int64(0))

	// After SleepWindow, circuit becomes half-open. Only one successful
	// case is needed to close the circuit.
	time.Sleep(IntToMillis(conf.SleepWindow))
	call.Run(func(args mock.Arguments) { /* no-op */ })
	_, err = stream.Get()
	assert.NoError(t, err)
	// In the end, circuit is closed because of no error.
}

func TestCircuitBreakerStream_Get3(t *testing.T) {
	// Test mockErr and subsequent ErrCircuitOpen
	circuit := xid.New().String()
	mstream := &mockStream{}
	count := 3

	conf := GetHystrixDefaultConfig()
	// Set RequestVolumeThreshold/ErrorPercentThreshold to be the most
	// sensitive. One request returns error would make the subsequent
	// requests coming within SleepWindow see ErrCircuitOpen
	conf.RequestVolumeThreshold = 1
	conf.ErrorPercentThreshold = 1
	conf.SleepWindow = 1000
	hystrix.ConfigureCommand(circuit, *conf)

	stream := NewCircuitBreakerStream("", "test", mstream, circuit)

	mm := &mockMsg{}
	mm.On("Id").Return("123")

	mockErr := errors.New("A mocked error")

	call := mstream.On("Get")
	call.Return(nil, mockErr)

	// 1st call gets mockErr which make subsequent requests within
	// SleepWindow see ErrCircuitOpen
	_, err := stream.Get()
	assert.EqualError(t, err, mockErr.Error())

	var called int64
	call.Run(func(args mock.Arguments) {
		atomic.StoreInt64(&called, 1)
	})
	for i := 0; i < count; i++ {
		_, err := stream.Get()
		assert.EqualError(t, err, ErrCircuitOpen.Error())
	}
	// ErrCircuitOpen prevents Get() from execution.
	assert.Equal(t, atomic.LoadInt64(&called), int64(0))

	// After SleepWindow, circuit becomes half-open. Only one successful
	// case is needed to close the circuit.
	time.Sleep(IntToMillis(conf.SleepWindow))
	call.Run(func(args mock.Arguments) { /* no-op */ })
	call.Return(mm, nil)
	_, err = stream.Get()
	assert.NoError(t, err)
}

func TestCircuitBreakerStream_Get4(t *testing.T) {
	// Test RequestVolumeThreshold/ErrorPercentThreshold
	circuit := xid.New().String()
	mstream := &mockStream{}
	count := 3

	conf := GetHystrixDefaultConfig()
	// Set ErrorPercentThreshold to be the most sensitive(1%). Once
	// RequestVolumeThreshold exceeded, the circuit becomes open.
	conf.RequestVolumeThreshold = count + 3
	conf.ErrorPercentThreshold = 1
	conf.SleepWindow = 1000
	hystrix.ConfigureCommand(circuit, *conf)

	stream := NewCircuitBreakerStream("","test", mstream, circuit)
	mockErr := errors.New("A mocked error")

	mm := &mockMsg{}
	mm.On("Id").Return("123")
	call := mstream.On("Get")

	// count is strictly smaller than RequestVolumeThreshold. So circuit is
	// still closed.
	for i := 0; i < count; i++ {
		call.Return(nil, mockErr)
		_, err := stream.Get()
		assert.EqualError(t, err, mockErr.Error())
	}

	// A success on the closed Circuit.
	call.Return(mm, nil)
	_, err := stream.Get()
	assert.NoError(t, err)

	// Until now, we have $count failed cases and 1 successful case. Need
	// $(RequestVolumeThreshold - count - 1) more cases(doesn't matter
	// successful or failed because ErrorPercentThreshold is the most
	// sensitive) to pass RequestVolumeThreshold to make the circuit open.
	for i := 0; i < conf.RequestVolumeThreshold; i++ {
		call.Return(mm, nil)
		_, err := stream.Get()
		if i < conf.RequestVolumeThreshold-count-1 {
			assert.NoError(t, err)
		} else {
			assert.EqualError(t, err, ErrCircuitOpen.Error())
		}
	}
}

func TestConcurrentFetchStream_Get(t *testing.T) {
	// This test shows that ConcurrentFetchStream reduces running time.
	max_concurrency := 5
	count := max_concurrency
	mm := &mockMsg{}
	mstream := &mockStream{}
	stream := NewConcurrentFetchStream("", "test", mstream, max_concurrency)

	suspend := 1 * time.Second
	mm.On("Id").Return("123")
	call := mstream.On("Get")
	call.Run(func(args mock.Arguments) {
		time.Sleep(suspend)
	})
	call.Return(mm, nil)
	begin := time.Now()
	for i := 0; i < count; i++ {
		_, err := stream.Get()
		assert.NoError(t, err)
	}
	dura := time.Now().Sub(begin)
	log.Info("[Test]", "dura", dura)
	assert.True(t, dura < suspend*time.Duration(max_concurrency))
}

func TestConcurrentFetchStream_Get2(t *testing.T) {
	// This test shows that ConcurrentFetchStream doesn't preserved order.
	count := 5
	cstream, msgs := genChannelStreamWithMessages(count)
	mhandler := &mockHandler{}
	mstream := NewMappedStream("","test", cstream, mhandler)

	calls := make([]*mock.Call, count)
	for i := 0; i < count; i++ {
		calls[i] = mhandler.On("Handle", msgs[i])
		calls[i].Run(func(args mock.Arguments) {
			m := args.Get(0).(Message)
			log.Info("[Test] handling", "msg", m.Id())
		})
		calls[i].Return(msgs[i], nil)
	}
	// Handler deals with the 1st longer than the others. So it's expected
	// to be pushed to the end of stream.
	calls[0].Run(func(args mock.Arguments) {
		m := args.Get(0).(Message)
		log.Info("[Test] handling slow", "msg", m.Id())
		time.Sleep(1 * time.Second)
	})
	stream := NewConcurrentFetchStream("", "test", mstream, 2)
	ids := make([]string, count)
	for i := 0; i < count; i++ {
		log.Info("[Test] loop", "i", i)
		msg, err := stream.Get()
		assert.NoError(t, err)
		log.Info("[Test] loop", "msg_out", msg.Id())
		ids[i] = msg.Id()
	}
	// The 1st msg from upstream is now the last
	assert.Equal(t, ids[count-1], "0")
}

