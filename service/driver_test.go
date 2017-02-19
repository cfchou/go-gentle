// vim:fileencoding=utf-8
package service

import (
	"flag"
	"testing"
	"github.com/inconshreveable/log15"
	"github.com/stretchr/testify/assert"
	"fmt"
	"sync"
	"time"
	"github.com/stretchr/testify/mock"
	"github.com/pkg/errors"
)

var log = Log.New()

func TestMain(m *testing.M) {
	flag.Parse()
	//h := log15.LvlFilterHandler(log15.LvlDebug, log15.CallerFuncHandler(log15.StdoutHandler))
	h := log15.LvlFilterHandler(log15.LvlDebug,
		log15.MultiHandler(
			log15.StdoutHandler,
			log15.Must.FileHandler("./test.log", log15.LogfmtFormat())))
	Log.SetHandler(h)
	m.Run()
}

type MockMsg struct {
	id string
}

func (m *MockMsg) Id() string {
	return m.id
}

type MockMsgs struct {
	id string
	msgs []Message
}

func (m *MockMsgs) Id() string {
	return m.id
}

func (m *MockMsgs) Flatten() []Message {
	return m.msgs
}

type MockDriver struct {
	mock.Mock
	log log15.Logger
}

func (m *MockDriver) Exchange(msg Message, timeout time.Duration) (Messages, error) {
	args := m.Called(msg, timeout)
	return args.Get(0).(Messages), args.Error(1)
}

func (m *MockDriver) Logger() log15.Logger {
	return m.log
}


// Generate Messages with $id. It contains []Message that each of which has id
// in the form of $id.$index
func genMessages(id string, num int) Messages {
	msgs := make([]Message, num)
	for i:= 0; i < num; i++ {
		msgs[i] = &MockMsg{
			id: string(i),
		}
	}
	return &MockMsgs{
		id:id,
		msgs:msgs,
	}
}

// Return two channels $src and $done.
// $src generates infinite amount of Messages until close($done).
// For every Messages $msgs from $src, $msg.Flatten() returns []Message of
// length $n_nested.
func genMessagesChannelInfinite(n_nested int) (<-chan *MessagesTuple, chan *struct{}) {
	done := make(chan *struct{}, 1)
	src := make(chan *MessagesTuple, 1)
	go func() {
		count := 1
		for {
			tp := &MessagesTuple{
				msgs:genMessages(fmt.Sprintf("#%d", count),
					n_nested),
				err:nil,
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

func TestChannelDriver_Exchange_1(t *testing.T) {
	id := "#1"
	msgs := genMessages("#1", 0)
	src := make(chan *MessagesTuple, 1)
	src <- &MessagesTuple{
		msgs:msgs,
		err:nil,
	}

	drv := NewChannelDriver("test", src)

	msg_in := &MockMsg{id:"#0"}
	msg_out, err := drv.Exchange(msg_in, 0)
	assert.NoError(t, err)
	assert.Equal(t, msg_out.Id(), id)
}

func TestChannelDriver_Exchange_2(t *testing.T) {
	src := make(chan *MessagesTuple, 1)
	count := 10
	go func() {
		for i:=1; i<=count; i++{
			msgs := genMessages(fmt.Sprintf("#%d", i), 0)
			src <- &MessagesTuple{
				msgs:msgs,
				err:nil,
			}
		}
		close(src)
	}()

	drv := NewChannelDriver("test", src)
	msg_in := &MockMsg{id:"#0"}
	for i:=1; i<=count; i++{
		msg_out, err := drv.Exchange(msg_in, 0)
		assert.NoError(t, err)
		assert.Equal(t, msg_out.Id(), fmt.Sprintf("#%d", i))
	}
	// one more Exchange() should see ErrEOF
	msg_out, err := drv.Exchange(msg_in, 0)
	assert.EqualError(t, err, ErrEOF.Error())
	assert.Nil(t, msg_out)
}

func TestRateLimitedDriver_Exchange(t *testing.T) {
	src, done := genMessagesChannelInfinite(0)
	// 1 msg/sec
	requests_interval := 1000
	drv := NewRateLimitedDriver("rate",
		NewChannelDriver("chan", src),
		NewTokenBucketRateLimit(requests_interval, 1))
	msg_in := &MockMsg{id:"#0"}
	count := 4
	minimum := time.Duration((count - 1) * requests_interval) * time.Millisecond
	var wg sync.WaitGroup
	wg.Add(count)
	begin := time.Now()
	for i := 0; i < count; i++ {
		go func() {
			_, err := drv.Exchange(msg_in, 0)
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
	mdrv := &MockDriver{log:log.New("mixin", "mock")}
	backoffs := []time.Duration{1*time.Second, 2*time.Second}
	drv := NewRetryDriver("retry", mdrv,
		func() []time.Duration{
			return backoffs
		})
	msg_in := &MockMsg{id:"#0"}
	msgs := genMessages(fmt.Sprintf("#%d", 1), 0)

	call := mdrv.On("Exchange", msg_in, IntToMillis(0))
	call.Return(msgs, nil)

	_, err := drv.Exchange(msg_in, 0)
	assert.NoError(t, err)

	mockErr := errors.New("A mocked error")
	call.Return(msgs, mockErr)

	minimum := func() time.Duration{
		dura_sum := 0 * time.Second
		for _, dura := range backoffs {
			dura_sum += dura
		}
		return dura_sum
	}()
	begin := time.Now()
	_, err = drv.Exchange(msg_in, 0)
	end := time.Now()
	assert.EqualError(t, err, mockErr.Error())
	log.Info("[Test] spent >= minmum?", "spent", end.Sub(begin), "minimum", minimum)
	assert.True(t, end.Sub(begin) >= minimum)
}

