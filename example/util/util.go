package util

import (
	"time"
	"os"
)

// Based on times_per_sec to calculate the interval in millis between every
// request.
func FreqToIntervalMillis(times_per_sec int) time.Duration {
	n := time.Millisecond / time.Duration(times_per_sec)
	if n < 1 {
		Log.Error("Interval should be no less than 1 millisec",
			"interval", n)
		os.Exit(-1)
	}
	Log.Info("Interval: %s\n", "interval", n)
	return n
}

// Based on times_per_sec to calculate the interval in micros between every
// request.
func FreqToIntervalMicros(times_per_sec int) time.Duration {
	n := time.Microsecond / time.Duration(times_per_sec)
	if n < 1 {
		Log.Error("Interval should be no less than 1 microsec",
			"interval", n)
		os.Exit(-1)
	}
	Log.Info("Interval: %s\n", "interval", n)
	return n
}
