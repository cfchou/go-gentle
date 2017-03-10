// vim:fileencoding=utf-8
package service

import (
	"github.com/stretchr/testify/mock"
	"testing"
	"github.com/stretchr/testify/assert"
	"fmt"
	"time"
	"sync"
	"errors"
)

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
	dura := time.Now().Sub(begin)
	log.Info("[Test] spent >= minmum?", "spent", dura, "minimum", minimum)
	assert.True(t, dura >= minimum)
	done <- &struct{}{}
}

func TestRetryStream_Receive(t *testing.T) {
	mstream := &mockStream{}
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

	// 1st: ok
	mm := &mockMsg{}
	mm.On("Id").Return("123")
	call := mstream.On("Receive")
	call.Return(mm, nil)

	_, err := stream.Receive()
	assert.NoError(t, err)

	// 2ed: err, trigger retry with backoffs
	mockErr := errors.New("A mocked error")
	call.Return(nil, mockErr)

	begin := time.Now()
	_, err = stream.Receive()
	dura := time.Now().Sub(begin)
	// backoffs exhausted
	assert.EqualError(t, err, mockErr.Error())
	log.Info("[Test] spent >= minmum?", "spent", dura, "minimum", minimum)
	assert.True(t, dura >= minimum)
}

func TestBulkheadStream_Receive(t *testing.T) {
	count := 8
	max_concurrency := 4
	mstream := &mockStream{}
	stream := NewBulkheadStream("bulk", mstream, max_concurrency)

	suspend := 1 * time.Second
	maximum := suspend * time.Duration((count + max_concurrency - 1) / max_concurrency) + time.Second

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
	begin := time.Now()
	for i :=0; i < count; i++ {
		go func () {
			msg, err := stream.Receive()
			wg.Done()
			assert.NoError(t, err)
			assert.Equal(t, msg.Id(), mm.Id())
		}()
	}
	wg.Wait()
	dura := time.Now().Sub(begin)
	log.Info("[Test] spent <= maximum?", "spent", dura, "maximum", maximum)
	assert.True(t, dura <= maximum)
}

func TestMappedStream_Receive(t *testing.T) {
	mstream := &mockStream{}
	mhandler := &mockHandler{}
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
	mstream := &mockStream{}
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
	mhandler := &mockHandler{}
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



