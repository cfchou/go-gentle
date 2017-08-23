package go_gentle

// Logger provides structural logging interface.
type Logger interface {
	// Log a message at the given level with key/value pairs. The number of
	// fields must be multiple of two for a key and a value.
	Debug(msg string, fields ...interface{})
	Info(msg string, fields ...interface{})
	Warn(msg string, fields ...interface{})
	Error(msg string, fields ...interface{})
	Crit(msg string, fields ...interface{})

	// Logger may support structured/contextual logging.
	New(fields ...interface{}) Logger
}



