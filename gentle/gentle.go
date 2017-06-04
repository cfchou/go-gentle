package gentle

import (
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	log15 "gopkg.in/inconshreveable/log15.v2"
	"time"
)

var (
	// Package level logger. It uses log15(gopkg.in/inconshreveable/log15.v2)
	// to provide finer control over logging.
	Log = log15.New()

	// Errors that CircuitBreakerStream.Get() and
	// CircuitBreakerHandler.Handle() might return. They are replacement of
	// hystrix errors.
	ErrCbOpen           = errors.New(hystrix.ErrCircuitOpen.Error())
	ErrCbMaxConcurrency = errors.New(hystrix.ErrMaxConcurrency.Error())
	ErrCbTimeout        = errors.New(hystrix.ErrTimeout.Error())

	ErrMaxConcurrency = errors.New("Reached Max Concurrency")
	ErrInvalidType    = errors.New("Invalid Type")
)

func init() {
	Log.SetHandler(log15.DiscardHandler())
}

type Message interface {
	// A Message is obliged to implement Id() for better tracing.
	Id() string
}

// errors that implements IgnorableError would be examined and ignored if
// necessary by RetryXXX and CircuitBreakerXXX.
type IgnorableError interface {
	Ignored() bool
}

func ToIgnore(err error) bool {
	if ig, ok := err.(IgnorableError); !ok {
		return false
	} else {
		return ig.Ignored()
	}
}

// Stream emits Message. Messages of a stream goes one way. Though two streams
// can simulate two-way communication but it would require out-of-band logic.
type Stream interface {
	// Get() returns either a Message or an error. Returned Message is nil
	// if only if error is not nil.
	Get() (Message, error)
}

// Handler transforms a Message.
type Handler interface {
	// Handle() transforms an Message. Returned Message is nil if only if
	// error is not nil.
	Handle(msg Message) (Message, error)
}

type Names struct {
	Namespace string
	Mixin     string
	Name      string
}

type Identity interface {
	GetNames() *Names
}

// A Logger writes key/value pairs to a Handler
type Logger interface {
	// Log a message at the given level with context key/value pairs
	Debug(msg string, ctx ...interface{})
	Info(msg string, ctx ...interface{})
	Warn(msg string, ctx ...interface{})
	Error(msg string, ctx ...interface{})
	Crit(msg string, ctx ...interface{})
}

// A do-nothing Logger
type noOpLogger struct{}

func (l *noOpLogger) Debug(msg string, ctx ...interface{}) {}
func (l *noOpLogger) Info(msg string, ctx ...interface{})  {}
func (l *noOpLogger) Warn(msg string, ctx ...interface{})  {}
func (l *noOpLogger) Error(msg string, ctx ...interface{}) {}
func (l *noOpLogger) Crit(msg string, ctx ...interface{})  {}

var noopLogger = &noOpLogger{}

// RateLimit is an interface for a "token bucket" algorithm.
type RateLimit interface {
	// Wait for $count tokens are granted(return true) or timeout(return
	// false). If $timeout == 0, it would block as long as it needs.
	Wait(count int, timeout time.Duration) bool
}

type BackOff interface {
	// Next() should immediately return
	Next() time.Duration
}

const BackOffStop time.Duration = -1

// Converts $millis of int to time.Duration.
func IntToMillis(millis int) time.Duration {
	return time.Duration(millis) * time.Millisecond
}

// GetHystrixDefaultConfig() returns a new hystrix.CommandConfig filled with defaults(https://godoc.org/github.com/afex/hystrix-go/hystrix#pkg-variables):
func GetHystrixDefaultConfig() *hystrix.CommandConfig {
	return &hystrix.CommandConfig{
		// DefaultTimeout = 1000, is how long to wait for command to complete, in milliseconds
		Timeout: hystrix.DefaultTimeout,
		// DefaultMaxConcurrent = 10 is how many commands of the same type can run at the same time
		MaxConcurrentRequests: hystrix.DefaultMaxConcurrent,
		// DefaultVolumeThreshold = 20 is the minimum number of requests needed before a circuit can be tripped due to health
		RequestVolumeThreshold: hystrix.DefaultVolumeThreshold,
		// DefaultSleepWindow = 5000 is how long, in milliseconds, to wait after a circuit opens before testing for recovery
		SleepWindow: hystrix.DefaultSleepWindow,
		// DefaultErrorPercentThreshold = 50 causes circuits to open once the rolling measure of errors exceeds this percent of requests
		ErrorPercentThreshold: hystrix.DefaultErrorPercentThreshold,
	}
}

type CircuitBreakerConf struct {
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

func NewDefaultCircuitBreakerConf() *CircuitBreakerConf {
	return &CircuitBreakerConf{
		Timeout:               10 * time.Second,
		MaxConcurrent:         1024,
		VolumeThreshold:       20,
		ErrorPercentThreshold: 50,
		SleepWindow:           5 * time.Second,
	}
}

func (c *CircuitBreakerConf) RegisterFor(circuit string) {
	hystrix.ConfigureCommand(circuit, hystrix.CommandConfig{
		Timeout:                int(c.Timeout / time.Millisecond),
		MaxConcurrentRequests:  c.MaxConcurrent,
		RequestVolumeThreshold: c.VolumeThreshold,
		SleepWindow:            int(c.SleepWindow / time.Millisecond),
		ErrorPercentThreshold:  c.ErrorPercentThreshold,
	})
}

type tuple struct {
	fst interface{}
	snd interface{}
}
