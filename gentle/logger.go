package gentle

import "gopkg.in/inconshreveable/log15.v2"

// Logger provides structural logging interface.
type Logger interface {
	// Log a message at the given level with key/value pairs. The number of
	// fields must be multiple of two for a key and a value.
	Debug(msg string, fields ...interface{})
	Info(msg string, fields ...interface{})
	Warn(msg string, fields ...interface{})
	Error(msg string, fields ...interface{})
	Crit(msg string, fields ...interface{})

	// Logger may support structured/contextual logging. Our Streams/Handlers
	// will call package global logger.New("namespace", "namespace_specified",
	// "name", "name_specified", "gentle", "stream/handler_type")
	New(fields ...interface{}) Logger
}

// Log is a package level logger. It's the parent logger of all loggers used
// by resilience Streams/Handlers defined in this package.
var Log Logger

type log15Logger struct {
	log15.Logger
}

func (l *log15Logger) New(fields ...interface{}) Logger {
	return &log15Logger{
		Logger: l.Logger.New(fields...),
	}
}
