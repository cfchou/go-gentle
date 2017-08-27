package gentle

import (
	"flag"
	"fmt"
	"gopkg.in/inconshreveable/log15.v2"
	"gopkg.in/inconshreveable/log15.v2/stack"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Parent logger for tests
var log Logger

type log15Logger struct {
	log15.Logger
}

func (l *log15Logger) New(fields ...interface{}) Logger {
	return &log15Logger{
		Logger: l.Logger.New(fields...),
	}
}

func TestMain(m *testing.M) {
	var level string
	flag.StringVar(&level, "level", "info", "log level")
	flag.Parse()
	rand.Seed(time.Now().UTC().UnixNano())
	//h := log15.LvlFilterHandler(log15.LvlDebug, log15.CallerFuncHandler(log15.StdoutHandler))
	//h := log15.LvlFilterHandler(log15.LvlDebug, callerFuncHandler(log15.StdoutHandler))
	h := log15.LvlFilterHandler(mapLogLevel(level), log15.StdoutHandler)
	//h := log15.LvlFilterHandler(log15.LvlDebug,
	//	log15.MultiHandler(
	//		log15.StdoutHandler,
	//		log15.Must.FileHandler("./test.log", log15.LogfmtFormat())))

	// replace package-global logger
	logger := log15.New()
	logger.SetHandler(h)
	Log = &log15Logger{Logger: logger}
	// init logger for test
	log = Log.New()

	m.Run()
}

func mapLogLevel(level string) log15.Lvl {
	switch strings.ToLower(level) {
	case "debug":
		return log15.LvlDebug
	case "warn", "warning":
		return log15.LvlWarn
	case "error":
		return log15.LvlError
	case "crit", "fatal":
		return log15.LvlCrit
	default:
		return log15.LvlInfo
	}
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

// Returns a $src of "chan Message" and $done chan of "chan *struct{}".
// Every Message extracted from $src has a monotonically increasing id.
func createInfiniteMessageChan() (<-chan Message, chan struct{}) {
	done := make(chan struct{}, 1)
	src := make(chan Message, 1)
	go func() {
		count := 0
		for {
			select {
			case <-done:
				log.Info("[Test] Channel closing")
				close(src)
				return
			default:
				count++
				src <- SimpleMessage(strconv.Itoa(count))
			}
		}
	}()
	return src, done
}
