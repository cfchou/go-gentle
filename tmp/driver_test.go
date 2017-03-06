// vim:fileencoding=utf-8
package service

import (
	"testing"
	"github.com/inconshreveable/log15"
	"github.com/stretchr/testify/assert"
	"fmt"
	"sync"
	"time"
	"github.com/stretchr/testify/mock"
	"github.com/pkg/errors"
)

type mockMsg struct {
	id string
}

func (m *mockMsg) Id() string {
	return m.id
}

var dummy_msg = &mockMsg{id: "#0"}

type mockMeta struct {
	id   string
	msgs []Message
}

func (m *mockMeta) Id() string {
	return m.id
}

func (m *mockMeta) Flatten() []Message {
	return m.msgs
}

type mockDriver struct {
	mock.Mock
	log log15.Logger
}

func (m *mockDriver) Exchange(msg Message, timeout time.Duration) (MetaMessage, error) {
	args := m.Called(msg, timeout)
	return args.Get(0).(MetaMessage), args.Error(1)
}

func (m *mockDriver) Logger() log15.Logger {
	return m.log
}

// Generate MetaMessage with $id. It contains []Message that each of which has
// an id in the form of $id.$index where $index ranges from 0 to
// $num_msgs_each_meta.
func genMetaMessage(id string, num_msgs_each_meta int) MetaMessage {
	msgs := make([]Message, num_msgs_each_meta)
	for i := 0; i < num_msgs_each_meta; i++ {
		msgs[i] = &mockMsg{
			id: fmt.Sprintf("%s.%d", id, i),
		}
	}
	return &mockMeta{
		id:   id,
		msgs: msgs,
	}
}

// Return two channels $src and $done.
// $src generates infinite amount of MetaMessage until close($done).
// For every MetaMessage $msgs from $src, $msg.Flatten() returns []Message of
// length 0.
func genMetaMessageChannelInfinite(num_msgs_each_meta int) (<-chan *MetaMessageTuple, chan *struct{}) {
	done := make(chan *struct{}, 1)
	src := make(chan *MetaMessageTuple, 1)
	go func() {
		count := 1
		for {
			metaMessage := genMetaMessage(fmt.Sprintf("#%d", count),
				num_msgs_each_meta)
			tp := &MetaMessageTuple{
				metaMessage: metaMessage,
				err: nil,
			}
			select {
			case <-done:
				log.Info("[Test] Channel closed")
				break
			case src <- tp:
			}
			count++
		}
		close(src)
	}()
	return src, done
}

func TestChannelDriver_Exchange_1(t *testing.T) {
	id := "#1"
	metaMessage := genMetaMessage(id, 0)
	src := make(chan *MetaMessageTuple, 1)
	src <- &MetaMessageTuple{
		metaMessage: metaMessage,
		err:         nil,
	}

	drv := NewChannelDriver("test", src)

	msg_out, err := drv.Exchange(dummy_msg, 0)
	assert.NoError(t, err)
	assert.Equal(t, msg_out.Id(), id)
}

func TestChannelDriver_Exchange_2(t *testing.T) {
	src := make(chan *MetaMessageTuple, 1)
	count := 10
	go func() {
		for i := 1; i <= count; i++ {
			metaMessage := genMetaMessage(fmt.Sprintf("#%d", i),
				0)
			src <- &MetaMessageTuple{
				metaMessage: metaMessage,
				err:         nil,
			}
		}
		close(src)
	}()

	drv := NewChannelDriver("test", src)
	for i := 1; i <= count; i++ {
		msg_out, err := drv.Exchange(dummy_msg, 0)
		assert.NoError(t, err)
		assert.Equal(t, msg_out.Id(), fmt.Sprintf("#%d", i))
	}
	// one more Exchange() should see ErrEOF
	msg_out, err := drv.Exchange(dummy_msg, 0)
	assert.EqualError(t, err, ErrEOF.Error())
	assert.Nil(t, msg_out)
}

func TestRateLimitedDriver_Exchange(t *testing.T) {
	src, done := genMetaMessageChannelInfinite(0)
	// 1 msg/sec
	requests_interval := 1000
	drv := NewRateLimitedDriver("rate",
		NewChannelDriver("chan", src),
		NewTokenBucketRateLimit(requests_interval, 1))
	count := 4
	minimum := time.Duration((count-1)*requests_interval) * time.Millisecond
	var wg sync.WaitGroup
	wg.Add(count)
	begin := time.Now()
	for i := 0; i < count; i++ {
		go func() {
			_, err := drv.Exchange(dummy_msg, 0)
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

func TestRetryDriver_Exchange(t *testing.T) {
	mdrv := &mockDriver{log: log.New("mixin", "mock")}
	backoffs := []time.Duration{1 * time.Second, 2 * time.Second}
	minimum := func() time.Duration {
		dura_sum := 0 * time.Second
		for _, dura := range backoffs {
			dura_sum += dura
		}
		return dura_sum
	}()
	drv := NewRetryDriver("retry", mdrv,
		func() []time.Duration {
			return backoffs
		})
	metaMessage := genMetaMessage(fmt.Sprintf("#%d", 1), 0)

	// 1st ok
	call := mdrv.On("Exchange", dummy_msg, IntToMillis(0))
	call.Return(metaMessage, nil)

	_, err := drv.Exchange(dummy_msg, 0)
	assert.NoError(t, err)

	// 2ed err, trigger retry
	mockErr := errors.New("A mocked error")
	call.Return(metaMessage, mockErr)

	begin := time.Now()
	_, err = drv.Exchange(dummy_msg, 0)
	end := time.Now()
	assert.EqualError(t, err, mockErr.Error())
	log.Info("[Test] spent >= minmum?", "spent", end.Sub(begin), "minimum", minimum)
	assert.True(t, end.Sub(begin) >= minimum)
}
