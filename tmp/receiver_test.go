// vim:fileencoding=utf-8
package service

import (
	"testing"
	"github.com/inconshreveable/log15"
	"github.com/stretchr/testify/mock"
	"time"
	"errors"
	"sync"
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

func _TestBackOffReceiver_ReceiveMessages_blocking(t *testing.T) {
	count := 8
	mon := &mockMonitor{}
	mon.On("NeedBackOff").Return(false)

	r := &mockReceiver{
		log: log.New(),
	}
	var wg sync.WaitGroup
	wg.Add(count)

	c := r.On("ReceiveMessages").Run(func (args mock.Arguments) {
		log.Debug("[Test]", "when", time.Now().Unix())
		time.Sleep(2 * time.Second)
		wg.Done()
	})

	br := NewBackOffReceiver(r, mon, 1 * time.Second)

	c.Return([]Message{}, errors.New("oh oh"))
	// synchronous call to switch on back-off
	br.ReceiveMessages()
	for i := 1; i < count; i++ {
		// asynchronous calls
		go br.ReceiveMessages()
	}
	wg.Wait()
}

func TestBackOffReceiver_ReceiveMessages_backoff_to_normal(t *testing.T) {
	mon := &mockMonitor{}
	mon.On("NeedBackOff").Return(false)

	r := &mockReceiver{
		log: log.New(),
	}

	var wg sync.WaitGroup
	wg.Add(4)
	c := r.On("ReceiveMessages").Run(func (args mock.Arguments) {
		log.Debug("[Test]", "when", time.Now().Unix())
		time.Sleep(2 * time.Second)
		wg.Done()
	})

	br := NewBackOffReceiver(r, mon, 1 * time.Second)

	log.Info("[Test] error...", "when", time.Now().Unix())
	c.Return([]Message{}, errors.New("oh oh"))
	// synchronous call to switch on back-off
	br.ReceiveMessages()
	br.ReceiveMessages()

	// sleep < backoff, so next ReceiveMessages() starts when the last backoff expires
	log.Info("[Test] ok...", "when", time.Now().Unix())
	c.Return([]Message{}, nil)
	br.ReceiveMessages()
	br.ReceiveMessages()
	wg.Wait()
}

