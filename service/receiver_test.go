// vim:fileencoding=utf-8
package service

import (
	"testing"
	"github.com/inconshreveable/log15"
	"github.com/stretchr/testify/mock"
	"time"
	"errors"
)


type mockReceiver struct {
	mock.Mock
	log log15.Logger
}

func (m *mockReceiver) ReceiveMessages() ([]Message, error) {
	args := m.Called()
	return args.Get(0).([]Message), args.Error(1)
}

func (m *mockReceiver) Logger() log15.Logger {
	return m.log
}

type mockMonitor struct {
	mock.Mock
}

func (m *mockMonitor) NeedBackOff() bool {
	args := m.Called()
	return args.Bool(0)
}

func TestBackOffReceiver_ReceiveMessages_sleep_big_than_backoff(t *testing.T) {
	count := 5
	mon := &mockMonitor{}
	mon.On("NeedBackOff").Return(false)

	r := &mockReceiver{
		log: log.New(),
	}

	c := r.On("ReceiveMessages").Run(func (args mock.Arguments) {
		log.Debug("[Test]", "when", time.Now().Unix())
		time.Sleep(2 * time.Second)
	})

	// sleep > backoff
	br := NewBackOffReceiver(r, mon, 1 * time.Second)

	c.Return([]Message{}, errors.New("oh oh"))
	br.ReceiveMessages()
	for i := 1; i < count; i++ {
		br.ReceiveMessages()
	}
}

func TestBackOffReceiver_ReceiveMessages_sleep_small_than_backoff(t *testing.T) {
	count := 5
	mon := &mockMonitor{}
	mon.On("NeedBackOff").Return(false)

	r := &mockReceiver{
		log: log.New(),
	}

	c := r.On("ReceiveMessages").Run(func (args mock.Arguments) {
		log.Debug("[Test]", "when", time.Now().Unix())
		time.Sleep(2 * time.Second)
	})

	// sleep < backoff
	br := NewBackOffReceiver(r, mon, 3 * time.Second)

	c.Return([]Message{}, errors.New("oh oh"))
	br.ReceiveMessages()
	for i := 1; i < count; i++ {
		br.ReceiveMessages()
	}
}

func TestBackOffReceiver_ReceiveMessages_backoff_to_normal(t *testing.T) {
	mon := &mockMonitor{}
	mon.On("NeedBackOff").Return(false)

	r := &mockReceiver{
		log: log.New(),
	}

	c := r.On("ReceiveMessages").Run(func (args mock.Arguments) {
		log.Debug("[Test]", "when", time.Now().Unix())
		time.Sleep(2 * time.Second)
	})

	// sleep < backoff
	br := NewBackOffReceiver(r, mon, 3 * time.Second)

	log.Info("[Test] error...", "when", time.Now().Unix())
	c.Return([]Message{}, errors.New("oh oh"))
	br.ReceiveMessages()
	br.ReceiveMessages()

	// sleep < backoff, so next ReceiveMessages() starts when the last backoff expires
	log.Info("[Test] ok when the last backoff expires", "when", time.Now().Unix())
	c.Return([]Message{}, nil)
	br.ReceiveMessages()
	br.ReceiveMessages()
}

