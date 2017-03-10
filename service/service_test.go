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
}

func (m *mockStream) Get() (Message, error) {
	args := m.Called()
	msg := args.Get(0)
	err := args.Get(1)
	if err != nil {
		return nil, err.(error)
	}
	return msg.(Message), nil
}

type mockHandler struct {
	mock.Mock
}

func (m *mockHandler) Handle(msg_in Message) (Message, error) {
	args := m.Called(msg_in)
	msg := args.Get(0)
	err := args.Get(1)
	if err != nil {
		return nil, err.(error)
	}
	return msg.(Message), nil
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

