// vim:fileencoding=utf-8
package service

import (
	"github.com/afex/hystrix-go/hystrix"
	"time"
	"sync/atomic"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/inconshreveable/log15"
	"github.com/cfchou/porter/service"
)

type SqsDownStreamConf struct {
	// circuit breaker
	Timeout                int `mapstructure:"timeout", json:"timeout"`
	RequestVolumeThreshold int `mapstructure:"request_volume_threshold", json:"request_volume_threshold"`
	ErrorPercentThreshold  int `mapstructure:"error_percent_threshold", json:"error_percent_threshold"`
	SleepWindow            int `mapstructure:"sleep_window", json:"sleep_window"`
	ConcurrentHandlers     int `mapstructure:"concurrent_handlers", json:"concurrent_handlers"`
}

// Implement both DownStream and DownStreamMonitor interfaces
type SqsDownStream struct {
	Name string
	Conf *SqsDownStreamConf

	log                    log15.Logger
	openedOrLastTestedTime int64
}

func NewSqsDownStream(name string, downConf SqsDownStreamConf) *SqsDownStream {
	// circuit for downstream
	hystrix.ConfigureCommand(name, hystrix.CommandConfig{
		MaxConcurrentRequests:  downConf.ConcurrentHandlers,
		Timeout:                downConf.Timeout,
		RequestVolumeThreshold: downConf.RequestVolumeThreshold,
		ErrorPercentThreshold:  downConf.ErrorPercentThreshold,
		SleepWindow:            downConf.SleepWindow,
	})
	return &SqsDownStream{
		Name:                   name,
		Conf:                   &downConf,
		log:                    Log.New("service", name),
		openedOrLastTestedTime: time.Now().Unix(),
	}
}

func (d *SqsDownStream) NeedBackOff() bool {
	// Circuit.AllowRequest() is unreliable because it has a race condition
	// that one call outside hystrix would make the one call inside hystrix
	// always return false, and vice versa.
	cb, _, _ := hystrix.GetCircuit(d.Name)
	now := time.Now().Unix()
	if !cb.IsOpen() {
		atomic.StoreInt64(&d.openedOrLastTestedTime, now)
		return false
	}
	lastTime := atomic.LoadInt64(&d.openedOrLastTestedTime)
	if time.Duration(now) > time.Duration(lastTime)+time.Duration(d.Conf.SleepWindow) {
		swapped := atomic.CompareAndSwapInt64(&d.openedOrLastTestedTime, lastTime, now)
		if swapped {
			d.log.Debug("[Down] NeedBackOff")
		}
		return swapped
	}
	return false
}

func (d *SqsDownStream) Run(up service.UpStream, handler func(interface{}) error) error {
	// Spawn no more than q.Conf.MaxWaitingMessages goroutines
	semaphore := make(chan *struct{}, d.Conf.ConcurrentHandlers)
	for {
		d.log.Debug("[Down] Dequeuing")
		m, _ := up.WaitMessage(0)
		semaphore <- &struct{}{}
		d.log.Debug("[Down] Semophore got", "sem_len", len(semaphore))
		done := make(chan *struct{}, 1)
		errChan := hystrix.Go(d.Name, func() error {
			msg, ok := m.(*sqs.Message)
			if !ok {
				panic("Not *Sqs.Message")
			}
			err := handler(msg)
			if err != nil {
				d.log.Error("[Down] err", "err", err)
				return err
			}
			d.log.Debug("[Down] ok")
			done <- &struct{}{}
			return nil
		}, func(err error) error {
			d.log.Warn("[Down] fallback", "err", err)
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
}
