// vim:fileencoding=utf-8
package service

import (
	"github.com/afex/hystrix-go/hystrix"
	"github.com/inconshreveable/log15"
	"sync/atomic"
)

var DownStreamLog = Log.New()

// Implement both DownStream and DownStreamMonitor interfaces
type DefaultDownStream struct {
	Name string
	log                    log15.Logger
	state int32
	concurrentHandlers int
}

func NewDefaultDownStream(name string, concurrentHandlers int) *DefaultDownStream {
	return &DefaultDownStream{
		Name:                   name,
		log: DownStreamLog.New("service", name),
		state: created,
		concurrentHandlers: concurrentHandlers,
	}
}

func (d *DefaultDownStream) Run(up UpStream, handler Handler) error {
	swapped := atomic.CompareAndSwapInt32(&d.state, created, running)
	if !swapped {
		return ErrRepeatedRun
	}
	settings := hystrix.GetCircuitSettings()
	if settings[d.Name].MaxConcurrentRequests != d.concurrentHandlers {
		return ErrConf
	}
	// Spawn no more than d.concurrentHandlers
	semaphore := make(chan *struct{}, d.concurrentHandlers)
	for {
		d.log.Debug("[Down] Dequeuing...")
		m, _ := up.WaitMessage(0)
		d.log.Debug("[Down] Dequeued, getting handler...", "msg", m.Id())
		semaphore <- &struct{}{}
		d.log.Debug("[Down] Handler goes", "concurrent_handlers", len(semaphore))
		done := make(chan *struct{}, 1)
		errChan := hystrix.Go(d.Name, func() error {
			err := handler(m)
			if err != nil {
				d.log.Error("[Down] Handler err", "err", err,
					"msg", m.Id())
				return err
			}
			d.log.Debug("[Down] Handler ok", "msg", m.Id())
			done <- &struct{}{}
			return nil
		}, func(err error) error {
			d.log.Warn("[Down] Fallback of handler", "err", err,
				"msg", m.Id())
			return err
		})
		go func() {
			select {
			case <-done:
			case <-errChan:
			}
			<-semaphore
		}()
	}
	panic("Never come here")
	return nil
}
