// vim:fileencoding=utf-8
package service

import (
	"sync"
	"github.com/inconshreveable/log15"
	"github.com/afex/hystrix-go/hystrix"
)

type GenMessage func() Message

// Turns a Driver to a Receiver. It keeps feeding Driver.Exchange() the same Message to get the rep
type DriverReceiver struct {
	Name         string
	driver       Driver
	log          log15.Logger
	msgs         chan interface{}
	gen_message   GenMessage
	once         sync.Once
}

func NewDriverReceiver(name string, driver Driver, max_queuing_messages int,
	gen_message GenMessage) *DriverReceiver {

	return &DriverReceiver{
		Name:         name,
		driver:       driver,
		log:          driver.Logger().New("mixin", name),
		msgs:         make(chan interface{}, max_queuing_messages),
		gen_message: gen_message,
	}
}

func (r *DriverReceiver) onceDo() {
	go func() {
		r.log.Info("[Receiver] once")
		for {
			// Viewed as an infinite source of events
			msgs, err := r.driver.Exchange(r.gen_message(), 0)
			if err != nil {
				r.msgs <- err
				continue
			}

			flattened_msgs := msgs.Flatten()
			for _, m := range flattened_msgs {
				r.msgs <- m
			}
		}
	}()
}

func (r *DriverReceiver) Receive() (Message, error) {
	r.once.Do(r.onceDo)
	switch m := (<-r.msgs).(type) {
	case error:
		return nil, m
	case Message:
		return m, nil
	default:
		panic("Never be here")
	}
}

// HandlerReceiver applies a Handler to the upstream Receiver.Receive().
type HandlerReceiver struct {
	Receiver
	Name string
	log       log15.Logger
	handler   Handler
	semaphore chan chan *tuple
	once sync.Once
}

type tuple struct {
	fst interface{}
	snd interface{}
}

func NewHandlerReceiver(name string, receiver Receiver, handler Handler,
	max_concurrent_handlers int) *HandlerReceiver {
	return &HandlerReceiver{
		Name:      name,
		Receiver:  receiver,
		log:       receiver.Logger().New("mixin", name),
		handler:   handler,
		semaphore: make(chan chan *tuple, max_concurrent_handlers),
	}
}

func (r *HandlerReceiver) onceDo() {
	go func() {
		r.log.Info("[Receiver] once")
		for {
			ret := make(chan *tuple, 1)
			msg, err := r.Receiver.Receive()
			if err != nil {
				r.semaphore <- ret
				go func() {
					ret <- &tuple{
						fst: msg,
						snd: err,
					}
				}()
				continue
			}
			r.semaphore <- ret
			go func() {
				// TODO: Wrapf(e)?
				m, e := r.handler(msg)
				ret <- &tuple{
					fst: m,
					snd: e,
				}
			}()
		}
	}()
}

func (r *HandlerReceiver) Receive() (Message, error) {
	r.once.Do(r.onceDo)
	ret := <-<-r.semaphore
	return ret.fst.(Message), ret.snd.(error)
}

type CircuitBreakerReceiver struct {
	Receiver
	Name string
	log       log15.Logger
}

func NewCircuitBreakerReceiver(name string, receiver Receiver,
	conf hystrix.CommandConfig) *CircuitBreakerReceiver {
	hystrix.ConfigureCommand(name, conf)
	return &CircuitBreakerReceiver{
		Receiver:receiver,
		Name:name,
		log:receiver.Logger().New("mixin", name),
	}
}

func (r *CircuitBreakerReceiver) Receive() (Message, error) {
	r.log.Debug("[Receiver] Receive()")
	result := make(chan *tuple, 1)
	err := hystrix.Do(r.Name, func() error {
		msg, err := r.Receiver.Receive()
		if err != nil {
			r.log.Error("[Receiver] Receive err", "err", err)
			result <- &tuple{
				fst: msg,
				snd: err,
			}
			return err
		}
		r.log.Debug("[Receiver] Receive ok", "msg_out", msg.Id())
		result <- &tuple{
			fst: msg,
			snd: err,
		}
		return nil
	}, nil)
	// hystrix.Do() is synchronous so at this point there are three
	// possibilities:
	// 1. work function is prevented from execution, err contains
	//    hystrix.ErrCircuitOpen or hystrix.ErrMaxConcurrency.
	// 2. work function is finished before hystrix's timeout. err is nil or
	//    an error returned by work.
	// 3. work function is finished after hystrix's timeout. err is
	//    hystrix.ErrTimeout or an error returned by work.
	// In case 2 and 3 we'd like to return Receive() if there's
	// any.
	// hystrix.ErrTimeout doesn't interrupt work anyway.
	// It just contributes to circuit's metrics.
	if err != nil {
		r.log.Warn("[Receiver] Circuit err","err", err)
		if err != hystrix.ErrTimeout {
			// Can be ErrCircuitOpen, ErrMaxConcurrency or
			// Receive()'s err.
			return nil, err
		}
	}
	tp := <-result
	return tp.fst.(Message), tp.snd.(error)
}




