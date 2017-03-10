// vim:fileencoding=utf-8
package service

import (
	"time"
	"github.com/inconshreveable/log15"
	"github.com/afex/hystrix-go/hystrix"
	"errors"
)

// Handlers, like Streams, come with resiliency patterns and can be mixed in
// each other.

type RateLimitedHandler struct {
	Handler
	Name string
	log log15.Logger
	limiter RateLimit
}

func NewRateLimitedHandler(name string, handler Handler, limiter RateLimit) *RateLimitedHandler {
	return &RateLimitedHandler{
		Handler: handler,
		Name:name,
		limiter:limiter,
		log:Log.New("mixin", "handler_rate", "name", name),
	}
}

func (r *RateLimitedHandler) Handle(msg Message) (Message, error) {
	r.log.Debug("[Handler] Handle()")
	r.limiter.Wait(1, 0)
	msg, err := r.Handler.Handle(msg)
	if err != nil {
		r.log.Error("[Handler] Handle err","err", err)
		return nil, err
	}
	r.log.Debug("[Handler] Handler ok","msg_out", msg.Id())
	return msg, nil
}

type RetryHandler struct {
	Handler
	Name string
	log log15.Logger
	genBackOff GenBackOff
}

func NewRetryHandler(name string, handler Handler, genBackOff GenBackOff) *RetryHandler {
	return &RetryHandler{
		Handler:handler,
		Name: name,
		genBackOff:genBackOff,
		log:Log.New("mixin", "handler_retry", "name", name),
	}
}

func (r *RetryHandler) Handle(msg Message) (Message, error) {
	r.log.Debug("[Handler] " ,
		"msg_in", msg.Id())
	var bk []time.Duration
	to_wait := 0 * time.Second
	count := 0
	for {
		count += 1
		r.log.Debug("[Handler] handler..." , "count", count,
			"wait", to_wait, "msg_in", msg.Id())
		// A negative or zero duration causes Sleep to return immediately.
		time.Sleep(to_wait)
		// assert end_allowed.Sub(now) != 0
		msg_out, err := r.Handler.Handle(msg)
		if err == nil {
			r.log.Debug("[Handler] handler ok","msg_in", msg.Id(),
				"msg_out", msg_out.Id())
			return msg, err
		}
		if count == 1 {
			bk = r.genBackOff()
			r.log.Debug("[Handler] generate backoffs",
				"len", len(bk), "msg_in", msg.Id())
		}
		if len(bk) == 0 {
			// backoffs exhausted
			r.log.Error("[Handler] handler err, stop backing off",
				"err", err, "msg_in", msg.Id())
			return nil, err
		} else {
			r.log.Error("[Handler] handler err",
				"err", err, "msg_in", msg.Id())
		}
		to_wait = bk[0]
		bk = bk[1:]
	}
}

type CircuitBreakerHandler struct {
	Handler
	Name string
	log       log15.Logger
}

func NewCircuitBreakerHandler(name string, handler Handler) *CircuitBreakerHandler {
	return &CircuitBreakerHandler{
		Handler:handler,
		Name:name,
		log:Log.New("mixin", "handler_circuit", "name", name),
	}
}

func (r *CircuitBreakerHandler) Handle(msg Message) (Message, error) {
	r.log.Debug("[Handler] Handle()")
	result := make(chan *tuple, 1)
	err := hystrix.Do(r.Name, func() error {
		msg_out, err := r.Handler.Handle(msg)
		if err != nil {
			r.log.Error("[Handler] Handle err", "err", err)
			result <- &tuple{
				fst: msg_out,
				snd: err,
			}
			return err
		}
		r.log.Debug("[Handler] Handle ok", "msg_out", msg_out.Id())
		result <- &tuple{
			fst: msg_out,
			snd: err,
		}
		return nil
	}, nil)
	// hystrix.ErrTimeout doesn't interrupt work anyway.
	// It just contributes to circuit's metrics.
	if err != nil {
		r.log.Warn("[Handler] Circuit err","err", err)
		if err != hystrix.ErrTimeout {
			// Can be ErrCircuitOpen, ErrMaxConcurrency or
			// Handle()'s err.
			return nil, err
		}
	}
	tp := <-result
	if tp.snd == nil {
		return tp.fst.(Message), nil
	}
	return nil, tp.snd.(error)
}

type BulkheadHandler struct {
	Handler
	Name string
	log       log15.Logger
	semaphore chan *struct{}
}

func NewBulkheadHandler(name string, handler Handler, max_concurrency int) *BulkheadHandler{
	if max_concurrency <= 0 {
		panic(errors.New("max_concurrent must be greater than 0"))
	}
	return &BulkheadHandler{
		Handler:handler,
		Name:name,
		log:Log.New("mixin", "handler_bulk", "name", name),
		semaphore: make(chan *struct{}, max_concurrency),
	}
}

func (r *BulkheadHandler) Handle(msg Message) (Message, error) {
	r.log.Debug("[Handler] " ,
		"msg_in", msg.Id())
	r.semaphore <- &struct{}{}
	defer func(){<- r.semaphore}()
	msg_out, err := r.Handler.Handle(msg)
	if err != nil {
		r.log.Error("[Handler] Handle err", "err", err, "msg_in",
			msg.Id())
	} else {
		r.log.Debug("[Handler] handler ok","msg_in", msg.Id(),
			"msg_out", msg_out.Id())
	}
	return msg_out, err
}
