package gentle

import (
	"flag"
	"fmt"
	"gopkg.in/inconshreveable/log15.v2"
	"gopkg.in/inconshreveable/log15.v2/stack"
	"math/rand"
	"reflect"
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
	//h := log15.LvlFilterHandler(log15.LvlDebug, callerFuncHandler(log15.StdoutHandler))
	h := log15.LvlFilterHandler(log15.LvlDebug, log15.StdoutHandler)
	//h := log15.LvlFilterHandler(log15.LvlDebug,
	//	log15.MultiHandler(
	//		log15.StdoutHandler,
	//		log15.Must.FileHandler("./test.log", log15.LogfmtFormat())))
	Log.SetHandler(h)
	m.Run()
}

// log15.CallerFuncHandler prints import-path-qualified function name whereas
// we want function name only.
// TODO:
// callerFuncHandler doesn't work when logger is wrapped in loggerFactory
func callerFuncHandler(h log15.Handler) log15.Handler {
	return log15.FuncHandler(func(r *log15.Record) error {
		call := stack.Call(r.CallPC[0])
		r.Ctx = append(r.Ctx, "fn", fmt.Sprintf("%n", call))
		return h.Log(r)
	})
}

// helper for package quick
// randomized value in the range [min, max)
func genBoundInt(min, max int) func(values []reflect.Value, rnd *rand.Rand) {
	if min > max {
		panic("Invalid argument")
	}
	n := max - min
	return func(values []reflect.Value, rnd *rand.Rand) {
		v := rnd.Intn(n)
		values[0] = reflect.ValueOf(v + min)
	}
}
