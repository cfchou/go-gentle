package gentle

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

type noopLogger struct{}

func (l *noopLogger) Debug(msg string, fields ...interface{}) {}
func (l *noopLogger) Info(msg string, fields ...interface{})  {}
func (l *noopLogger) Warn(msg string, fields ...interface{})  {}
func (l *noopLogger) Error(msg string, fields ...interface{}) {}
func (l *noopLogger) Crit(msg string, fields ...interface{})  {}
func (l *noopLogger) New(fields ...interface{}) Logger        { return l }
