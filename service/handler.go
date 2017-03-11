package service

import (
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/inconshreveable/log15"
	"time"
)

// Rate limiting pattern is used to limit the speed of a series of Handle().
type RateLimitedHandler struct {
	Name    string
	handler Handler
	log     log15.Logger
	limiter RateLimit
}

func NewRateLimitedHandler(name string, handler Handler, limiter RateLimit) *RateLimitedHandler {
	return &RateLimitedHandler{
		Name:    name,
		handler: handler,
		limiter: limiter,
		log:     Log.New("mixin", "handler_rate", "name", name),
	}
}

// Handle() is blocked when the limit is exceeded.
func (r *RateLimitedHandler) Handle(msg Message) (Message, error) {
	r.log.Debug("[Handler] Handle()")
	r.limiter.Wait(1, 0)
	msg, err := r.handler.Handle(msg)
	if err != nil {
		r.log.Error("[Handler] Handle err", "err", err)
		return nil, err
	}
	r.log.Debug("[Handler] Handler ok", "msg_out", msg.Id())
	return msg, nil
}

type RetryHandler struct {
	Name       string
	handler    Handler
	log        log15.Logger
	genBackOff GenBackOff
}

func NewRetryHandler(name string, handler Handler, genBackOff GenBackOff) *RetryHandler {
	return &RetryHandler{
		Name:       name,
		handler:    handler,
		genBackOff: genBackOff,
		log:        Log.New("mixin", "handler_retry", "name", name),
	}
}

func (r *RetryHandler) Handle(msg Message) (Message, error) {
	r.log.Debug("[Handler] ",
		"msg_in", msg.Id())
	var bk []time.Duration
	to_wait := 0 * time.Second
	count := 0
	for {
		count += 1
		r.log.Debug("[Handler] handler...", "count", count,
			"wait", to_wait, "msg_in", msg.Id())
		// A negative or zero duration causes Sleep to return immediately.
		time.Sleep(to_wait)
		// assert end_allowed.Sub(now) != 0
		msg_out, err := r.handler.Handle(msg)
		if err == nil {
			r.log.Debug("[Handler] handler ok", "msg_in", msg.Id(),
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
	Name    string
	Circuit string
	handler Handler
	log     log15.Logger
}

func NewCircuitBreakerHandler(name string, handler Handler, circuit string) *CircuitBreakerHandler {
	return &CircuitBreakerHandler{
		Name:    name,
		handler: handler,
		Circuit: circuit,
		log: Log.New("mixin", "handler_circuit", "name", name,
			"circuit", circuit),
	}
}

func (r *CircuitBreakerHandler) Handle(msg Message) (Message, error) {
	r.log.Debug("[Handler] Handle()")
	result := make(chan *tuple, 1)
	err := hystrix.Do(r.Circuit, func() error {
		msg_out, err := r.handler.Handle(msg)
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
		r.log.Warn("[Handler] Circuit err", "err", err)
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

// Bulkhead pattern is used to limit the number of concurrent Handle().
type BulkheadHandler struct {
	Name      string
	handler   Handler
	log       log15.Logger
	semaphore chan *struct{}
}

func NewBulkheadHandler(name string, handler Handler, max_concurrency int) *BulkheadHandler {
	if max_concurrency <= 0 {
		panic(errors.New("max_concurrent must be greater than 0"))
	}
	return &BulkheadHandler{
		Name:      name,
		handler:   handler,
		log:       Log.New("mixin", "handler_bulk", "name", name),
		semaphore: make(chan *struct{}, max_concurrency),
	}
}

// Handle() is blocked when the limit is exceeded.
func (r *BulkheadHandler) Handle(msg Message) (Message, error) {
	r.log.Debug("[Handler] ",
		"msg_in", msg.Id())
	r.semaphore <- &struct{}{}
	defer func() { <-r.semaphore }()
	msg_out, err := r.handler.Handle(msg)
	if err != nil {
		r.log.Error("[Handler] Handle err", "err", err, "msg_in",
			msg.Id())
	} else {
		r.log.Debug("[Handler] handler ok", "msg_in", msg.Id(),
			"msg_out", msg_out.Id())
	}
	return msg_out, err
}
