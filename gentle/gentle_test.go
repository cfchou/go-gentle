package gentle

import (
	"flag"
	"github.com/stretchr/testify/mock"
	log15 "gopkg.in/inconshreveable/log15.v2"
	"testing"
	"time"
	"sync"
)

// Parent logger for tests
var log = Log.New()

type fakeMsg struct {
	id string
}

func (m *fakeMsg) Id() string {
	return m.id
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
	h := log15.LvlFilterHandler(log15.LvlDebug, log15.CallerFuncHandler(log15.StdoutHandler))
	//h := log15.LvlFilterHandler(log15.LvlDebug,
	//	log15.MultiHandler(
	//		log15.StdoutHandler,
	//		log15.Must.FileHandler("./test.log", log15.LogfmtFormat())))
	Log.SetHandler(h)
	m.Run()
}
