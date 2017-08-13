package gentle

import (
	"flag"
	"fmt"
	"gopkg.in/inconshreveable/log15.v2"
	"gopkg.in/inconshreveable/log15.v2/stack"
	"math/rand"
	"testing"
	"time"
)

// Parent logger for tests
var log = Log.New()

type fakeMsg struct {
	id string
}

func (m *fakeMsg) ID() string {
	return m.id
}

func TestMain(m *testing.M) {
	// TODO flags for log config
	flag.Parse()
	rand.Seed(time.Now().UTC().UnixNano())
	//h := log15.LvlFilterHandler(log15.LvlDebug, log15.CallerFuncHandler(log15.StdoutHandler))
	h := log15.LvlFilterHandler(log15.LvlDebug, callerFuncHandler(log15.StdoutHandler))
	//h := log15.LvlFilterHandler(log15.LvlDebug,
	//	log15.MultiHandler(
	//		log15.StdoutHandler,
	//		log15.Must.FileHandler("./test.log", log15.LogfmtFormat())))
	Log.SetHandler(h)
	m.Run()
}

// log15.CallerFuncHandler prints import-path-qualified function name whereas
// we want function name only.
func callerFuncHandler(h log15.Handler) log15.Handler {
	return log15.FuncHandler(func(r *log15.Record) error {
		call := stack.Call(r.CallPC[0])
		r.Ctx = append(r.Ctx, "fn", fmt.Sprintf("%n", call))
		return h.Log(r)
	})
}
