// vim:fileencoding=utf-8
package service

import (
	"github.com/afex/hystrix-go/hystrix"
	"time"
	"sync/atomic"
	"github.com/inconshreveable/log15"
)

type CircuitBreakerMonitor struct {
	Name string

	log log15.Logger
	sleepWindow int
	openedOrLastTestedTime int64
}

func (d *CircuitBreakerMonitor) NeedBackOff() bool {
	// Circuit.AllowRequest() is unreliable because it has a race condition
	// that one call outside hystrix would make the one call inside hystrix
	// return false, and vice versa.
	cb, _, _ := hystrix.GetCircuit(d.Name)
	now := time.Now().Unix()
	if !cb.IsOpen() {
		atomic.StoreInt64(&d.openedOrLastTestedTime, now)
		return false
	}
	// Circuit is closed.
	lastTime := atomic.LoadInt64(&d.openedOrLastTestedTime)
	if time.Duration(now) <= time.Duration(lastTime)+time.Duration(d.sleepWindow) {
		return true
	}
	// Circuit is half-opened.
	// CAS because monitor may be tested concurrently. Allowing them all
	// might cause circuit to open again.
	swapped := atomic.CompareAndSwapInt64(&d.openedOrLastTestedTime, lastTime, now)
	if swapped {
		d.log.Debug("[Monitor] NeedBackOff")
	}
	return swapped
}

