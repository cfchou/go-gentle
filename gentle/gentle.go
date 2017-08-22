package gentle

import (
	"context"
	"errors"
	"github.com/cfchou/hystrix-go/hystrix"
	"gopkg.in/inconshreveable/log15.v2"
	"time"
)

const (
	// Types of resilience, are most often used with namespace & name to form a
	// key.
	// Since there's only one method for Stream/Handler, they are also used as
	// operation names for opentracing spans.
	StreamRateLimited  = "sRate"
	StreamRetry        = "sRetry"
	StreamBulkhead     = "sBulk"
	StreamCircuit      = "sCircuit"
	HandlerRateLimited = "hRate"
	HandlerRetry       = "hRetry"
	HandlerBulkhead    = "hBulk"
	HandlerCircuit     = "hCircuit"
)

var (
	// Errors related to CircuitStream/CircuitHandler. They are
	// the replacement of underlying errors of package hystrix.

	// ErrCbOpen suggests the circuit is opened.
	ErrCbOpen = errors.New(hystrix.ErrCircuitOpen.Error())

	// ErrCbMaxConcurrency suggests the circuit has reached its maximum
	// concurrency of operations.
	ErrCbMaxConcurrency = errors.New(hystrix.ErrMaxConcurrency.Error())

	// ErrCbTimeout suggests the operation has run for too long.
	ErrCbTimeout = errors.New(hystrix.ErrTimeout.Error())

	// ErrMaxConcurrency suggests BulkheadStream/BulkheadHandler has reached
	// its maximum concurrency of operations.
	ErrMaxConcurrency = errors.New("Reached Max Concurrency")
)

func init() {
	// Discard handler when package is being loaded. You may set up the
	// exported Log later.
	logger := log15.New()
	logger.SetHandler(log15.DiscardHandler())
	Log = &log15Logger{Logger: logger}
}

// Message is passed around Streams/Handlers.
type Message interface {
	// ID() should return a unique string representing this Message.
	ID() string
}

// SimpleMessage essentially wraps a string to be a Message.
type SimpleMessage string

func (m SimpleMessage) ID() string {
	return string(m)
}

// Stream emits Message.
type Stream interface {
	// Get() returns either a Message or an error exclusively.
	Get(context.Context) (Message, error)
}

// Handler transforms a Message.
type Handler interface {
	// Handle() takes a Message as input and then returns either a Message or
	// an error exclusively.
	Handle(context.Context, Message) (Message, error)
}

// CircuitReset resets all states(incl. metrics) of all circuits.
func CircuitReset() {
	// TODO:
	// only flush the given circuit
	hystrix.Flush()
}

type CircuitConf struct {
	// Timeout is how long to wait for command to complete
	Timeout time.Duration
	// MaxConcurrent is how many commands of the same type can run
	// at the same time
	MaxConcurrent int
	// VolumeThreshold is the minimum number of requests needed
	// before a circuit can be tripped due to health
	VolumeThreshold int
	// ErrorPercentThreshold causes circuits to open once the
	// rolling measure of errors exceeds this percent of requests
	ErrorPercentThreshold int
	// SleepWindow is how long to wait after a circuit opens before testing
	// for recovery is allowed
	SleepWindow time.Duration
}

const (
	// Default configuration for a circuitbreaker
	DefaultCbTimeout             = 10 * time.Second
	DefaultCbMaxConcurrent       = 1024
	DefaultCbVolumeThreshold     = 20
	DefaultCbErrPercentThreshold = 50
	DefaultCbSleepWindow         = 5 * time.Second
)

func NewDefaultCircuitConf() *CircuitConf {
	return &CircuitConf{
		Timeout:               DefaultCbTimeout,
		MaxConcurrent:         DefaultCbMaxConcurrent,
		VolumeThreshold:       DefaultCbVolumeThreshold,
		ErrorPercentThreshold: DefaultCbErrPercentThreshold,
		SleepWindow:           DefaultCbSleepWindow,
	}
}

func (c *CircuitConf) RegisterFor(circuit string) {
	hystrix.ConfigureCommand(circuit, hystrix.CommandConfig{
		Timeout:                int(c.Timeout / time.Millisecond),
		MaxConcurrentRequests:  c.MaxConcurrent,
		RequestVolumeThreshold: c.VolumeThreshold,
		SleepWindow:            int(c.SleepWindow / time.Millisecond),
		ErrorPercentThreshold:  c.ErrorPercentThreshold,
	})
}
