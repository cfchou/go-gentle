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
}

// noOpLogger logs nothing
type noOpLogger struct{}

func (l *noOpLogger) Debug(msg string, fields ...interface{}) {}
func (l *noOpLogger) Info(msg string, fields ...interface{})  {}
func (l *noOpLogger) Warn(msg string, fields ...interface{})  {}
func (l *noOpLogger) Error(msg string, fields ...interface{}) {}
func (l *noOpLogger) Crit(msg string, fields ...interface{})  {}

// noopLogger is a single instance of noOpLogger.
var noopLogger = &noOpLogger{}
