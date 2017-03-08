// vim:fileencoding=utf-8
package service

import (
	"github.com/stretchr/testify/mock"
	"github.com/inconshreveable/log15"
	"testing"
	"github.com/stretchr/testify/assert"
	"fmt"
	"time"
	"sync"
	"errors"
)

type mockMsg struct {
	mock.Mock
}

func (m *mockMsg) Id() string {
	args := m.Called()
	return args.Get(0).(string)
}

type mockStream struct {
	mock.Mock
	log log15.Logger
}

func (m *mockStream) Receive() (Message, error) {
	args := m.Called()
	return args.Get(0).(Message), args.Error(1)
}

func (m *mockStream) Logger() log15.Logger {
	return m.log
}

type mockHandler struct {
	mock.Mock
	log log15.Logger
}

func (m *mockHandler) Handle(msg Message) (Message, error) {
	args := m.Called(msg)
	return args.Get(0).(Message), args.Error(1)
}

func (m *mockHandler) Logger() log15.Logger {
	return m.log
}

func genMessageChannelInfinite() (<-chan Message, chan *struct{}) {
	done := make(chan *struct{}, 1)
	src := make(chan Message, 1)
	go func() {
		count := 1
		for {
			mm := &mockMsg{}
			mm.On("Id").Return(fmt.Sprint(count))
			select {
			case <-done:
				log.Info("[Test] Channel closed")
				break
			case src <- mm:
			}
		}
		close(src)
	}()
	return src, done
}

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
	return NewChannelStream("test", src), msgs
}

func TestChannelStream_Receive(t *testing.T) {
	mm := &mockMsg{}
	mm.On("Id").Return("123")
	src := make(chan Message, 1)
	src <- mm
	stream := NewChannelStream("test", src)
	msg_out, err := stream.Receive()
	assert.NoError(t, err)
	assert.Equal(t, msg_out.Id(), mm.Id())
}

func TestChannelStream_Receive_2(t *testing.T) {
	count := 10
	stream, msgs := genChannelStreamWithMessages(count)

	for i := 0; i < count; i++ {
		msg_out, err := stream.Receive()
		assert.NoError(t, err)
		assert.Equal(t, msg_out.Id(), msgs[i].Id())
	}
}

func TestRateLimitedStream_Receive(t *testing.T) {
	src, done := genMessageChannelInfinite()
	// 1 msg/sec
	requests_interval := 1000
	stream := NewRateLimitedStream("rate",
		NewChannelStream("chan", src),
		NewTokenBucketRateLimit(requests_interval, 1))
	count := 4
	minimum := time.Duration((count-1)*requests_interval) * time.Millisecond
	var wg sync.WaitGroup
	wg.Add(count)
	begin := time.Now()
	for i := 0; i < count; i++ {
		go func() {
			_, err := stream.Receive()
			assert.NoError(t, err)
			wg.Done()
		}()
	}
	wg.Wait()
	end := time.Now()
	log.Info("[Test] spent >= minmum?", "spent", end.Sub(begin), "minimum", minimum)
	assert.True(t, end.Sub(begin) >= minimum)
	done <- &struct{}{}
}

func TestRetryStream_Receive(t *testing.T) {
	mstream := &mockStream{log: log.New("mixin", "mock")}
	backoffs := []time.Duration{1 * time.Second, 2 * time.Second}
	minimum := func(backoffs []time.Duration) time.Duration {
		dura_sum := 0 * time.Second
		for _, dura := range backoffs {
			dura_sum += dura
		}
		return dura_sum
	}(backoffs)
	stream := NewRetryStream("retry", mstream,
		func() []time.Duration {return backoffs})

	// 1st ok
	mm := &mockMsg{}
	mm.On("Id").Return("123")
	call := mstream.On("Receive")
	call.Return(mm, nil)

	_, err := stream.Receive()
	assert.NoError(t, err)

	// 2ed err, trigger retry with backoffs
	mockErr := errors.New("A mocked error")
	call.Return(&mockMsg{}, mockErr)

	begin := time.Now()
	_, err = stream.Receive()
	end := time.Now()
	// backoffs exhausted
	assert.EqualError(t, err, mockErr.Error())
	log.Info("[Test] spent >= minmum?", "spent", end.Sub(begin), "minimum", minimum)
	assert.True(t, end.Sub(begin) >= minimum)
}

func TestBulkheadStream_Receive(t *testing.T) {
	count := 5
	max_concurrency := 2
	mstream := &mockStream{log: log.New("mixin", "mock")}
	stream := NewBulkheadStream("bulk", mstream, max_concurrency)

	suspend := 1 * time.Second
	tm := time.NewTimer(suspend)
	mm := &mockMsg{}
	mm.On("Id").Return("123")
	calling := 0
	call := mstream.On("Receive")
	call.Run(func (args mock.Arguments) {
		calling++
		time.Sleep(suspend)
	})
	call.Return(mm, nil)

	var wg sync.WaitGroup
	wg.Add(count)
	for i :=0; i < count; i++ {
		go func () {
			msg, err := stream.Receive()
			wg.Done()
			assert.NoError(t, err)
			assert.Equal(t, msg.Id(), mm.Id())
		}()
	}
	func() {
		for {
			select {
			case <-tm.C:
				log.Info("[Test] timeout")
				return
			default:
				log.Info("[Test] calling <= max_concurrency?",
					"calling", calling,
					"max_concurrency", max_concurrency)
				assert.True(t, calling <= max_concurrency)
			}
		}
	}()
	wg.Wait()
	assert.Equal(t, calling, count)
}

func TestMappedStream_Receive(t *testing.T) {
	mstream := &mockStream{log: log.New("mixin", "mock")}
	mhandler := &mockHandler{log: log.New("mixin", "mock")}
	mm := &mockMsg{}

	stream := NewMappedStream("test", mstream, mhandler)

	call := mm.On("Id")
	call.Return("123")
	receive := mstream.On("Receive")
	receive.Return(mm, nil)
	handle := mhandler.On("Handle", mm)
	handle.Run(func(args mock.Arguments) {
		log.Info("[Test] handle")
		call.Return("456")
	})
	handle.Return(mm, nil)

	msg, err := stream.Receive()
	assert.NoError(t, err)
	assert.Equal(t, msg.Id(), "456")
}

func TestConcurrentFetchStream_Receive(t *testing.T) {
	// This test shows that ConcurrentFetchStream reduces running time.
	max_concurrency := 5
	count := max_concurrency
	mm := &mockMsg{}
	mstream := &mockStream{log: log.New("mixin", "mock")}
	stream := NewConcurrentFetchStream("test", mstream, max_concurrency)

	suspend := 1 * time.Second
	mm.On("Id").Return("123")
	call := mstream.On("Receive")
	call.Run(func (args mock.Arguments) {
		time.Sleep(suspend)
	})
	call.Return(mm, nil)
	begin := time.Now()
	for i := 0; i < count; i++ {
		_, err := stream.Receive()
		assert.NoError(t, err)
	}
	dura := time.Now().Sub(begin)
	log.Info("[Test]", "dura", dura)
	assert.True(t, dura < suspend * time.Duration(max_concurrency))
}

func TestConcurrentFetchStream_Receive2(t *testing.T) {
	// This test shows that ConcurrentFetchStream doesn't preserved order.
	count := 5
	cstream, msgs := genChannelStreamWithMessages(count)
	mhandler := &mockHandler{log: log.New("mixin", "mock")}
	mstream := NewMappedStream("test", cstream, mhandler)

	calls := make([]* mock.Call, count)
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
	stream := NewConcurrentFetchStream("test", mstream, 2)
	ids := make([]string, count)
	for i := 0; i < count; i++ {
		log.Info("[Test] loop", "i", i)
		msg, err := stream.Receive()
		assert.NoError(t, err)
		log.Info("[Test] loop", "msg_out", msg.Id())
		ids[i] = msg.Id()
	}
	// The 1st msg from upstream is now the last
	assert.Equal(t, ids[count - 1], "0")
}



