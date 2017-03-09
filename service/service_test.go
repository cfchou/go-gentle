// vim:fileencoding=utf-8
package service

import (
	"flag"
	"testing"
	"github.com/inconshreveable/log15"
	"github.com/stretchr/testify/mock"
)

// Parent logger for tests
var log = Log.New()

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

