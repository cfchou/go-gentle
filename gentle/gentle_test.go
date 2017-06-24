package gentle

import (
	"flag"
	"gopkg.in/inconshreveable/log15.v2"
	"testing"
)

// Parent logger for tests
var log = Log.New()

type fakeMsg struct {
	id string
}

func (m *fakeMsg) Id() string {
	return m.id
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
