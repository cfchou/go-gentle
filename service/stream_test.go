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

func TestChannelStream_Receive_1(t *testing.T) {
	// Generate MetaMessage with $id. It contains []Message that each of
	// which has an id in the form of $id.$index where $index ranges from
	// 0 to $num.
	metaMessage := genMetaMessage("#1", 1)
	expected_id := fmt.Sprintf("#1.%d", 0)

	src := make(chan *MessageTuple, 1)
	src <- &MessageTuple{
		msg: metaMessage.Flatten()[0],
		err: nil,
	}

	stream := NewChannelStream("test", src)
	msg_out, err := stream.Receive()
	assert.NoError(t, err)
	assert.Equal(t, msg_out.Id(), expected_id)
}

func TestChannelStream_Receive_2(t *testing.T) {
	count := 10
	metaMessage := genMetaMessage("#1", count)
	msgs := metaMessage.Flatten()

	src := make(chan *MessageTuple, 1)
	go func() {
		for i := 0; i < count; i++ {
			src <- &MessageTuple{
				msg: msgs[i],
				err: nil,
			}
		}
		close(src)
	}()

	stream := NewChannelStream("test", src)
	for i := 0; i < count; i++ {
		expected_id := fmt.Sprintf("#1.%d", i)
		msg_out, err := stream.Receive()
		assert.NoError(t, err)
		assert.Equal(t, msg_out.Id(), expected_id)
	}
	// one more Receive() should see ErrEOF
	msg_out, err := stream.Receive()
	assert.EqualError(t, err, ErrEOF.Error())
	assert.Nil(t, msg_out)
}

func genMessageChannelInfinite() (<-chan *MessageTuple, chan *struct{}) {
	done := make(chan *struct{}, 1)
	src := make(chan *MessageTuple, 1)
	go func() {
		count := 1
		for {
			metaMessage := genMetaMessage(fmt.Sprintf("#%d", count),
				0)
			tp := &MessageTuple{
				// MetaMessage itself is a Message
				msg: metaMessage,
				err: nil,
			}
			select {
			case <-done:
				log.Info("[Test] Channel closed")
				break
			case src <- tp:
			}
		}
		close(src)
	}()
	return src, done
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
	minimum := func() time.Duration {
		dura_sum := 0 * time.Second
		for _, dura := range backoffs {
			dura_sum += dura
		}
		return dura_sum
	}()
	stream := NewRetryStream("retry", mstream,
		func() []time.Duration {return backoffs})

	metaMessage := genMetaMessage(fmt.Sprintf("#%d", 1), 0)

	// 1st ok
	call := mstream.On("Receive")
	call.Return(metaMessage, nil)

	_, err := stream.Receive()
	assert.NoError(t, err)

	// 2ed err, trigger retry
	mockErr := errors.New("A mocked error")
	call.Return(metaMessage, mockErr)

	begin := time.Now()
	_, err = stream.Receive()
	end := time.Now()
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
	calling := 0
	call := mstream.On("Receive")
	call.Run(func (args mock.Arguments) {
		calling++
		time.Sleep(suspend)
	})
	call.Return(dummy_msg, nil)

	var wg sync.WaitGroup
	wg.Add(count)
	for i :=0; i < count; i++ {
		go func () {
			msg, err := stream.Receive()
			wg.Done()
			assert.NoError(t, err)
			assert.Equal(t, msg.Id(), dummy_msg.Id())
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

func TestConcurrentFetchStream_Receive(t *testing.T) {
	max_concurrency := 2
	mstream := &mockStream{log: log.New("mixin", "mock")}
	stream := NewConcurrentFetchStream("bulk", mstream, max_concurrency)

	calling := 0
	call := mstream.On("Receive")
	call.Run(func (args mock.Arguments) {
		calling++
	})
	call.Return(dummy_msg, nil)

	msg, err := stream.Receive()
	assert.NoError(t, err)
	assert.Equal(t, msg.Id(), dummy_msg.Id())
	assert.Equal(t, calling, max_concurrency)
}

func TestMappedStream_Receive(t *testing.T) {

}

