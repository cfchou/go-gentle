package gentle

import (
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	log15 "gopkg.in/inconshreveable/log15.v2"
	"time"
)

// Rate limiting pattern is used to limit the speed of a series of Handle().
type RateLimitedHandler struct {
	Name    string
	Log     log15.Logger
	handler Handler
	limiter RateLimit
}

func NewRateLimitedHandler(name string, handler Handler, limiter RateLimit) *RateLimitedHandler {
	return &RateLimitedHandler{
		Name:    name,
		Log:     Log.New("mixin", "handler_rate", "name", name),
		handler: handler,
		limiter: limiter,
	}
}

// Handle() is blocked when the limit is exceeded.
func (r *RateLimitedHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Handler] Handle() ...", "msg_in", msg.Id())
	r.limiter.Wait(1, 0)
	msg_out, err := r.handler.Handle(msg)
	timespan := time.Now().Sub(begin).Seconds()
	if err != nil {
		r.Log.Error("[Handler] Handle() err", "msg_in", msg.Id(),
			"err", err, "timespan", timespan)
		return nil, err
	}
	r.Log.Debug("[Handler] Handle() ok", "msg_in", msg.Id(),
		"msg_out", msg_out.Id(), "timespan", timespan)
	return msg_out, nil
}

// RetryHandler takes an Handler. When Handler.Handle() encounters an error,
// RetryHandler back off for some time and then retries.
type RetryHandler struct {
	Name     string
	Log      log15.Logger
	handler  Handler
	backoffs []time.Duration
}

func NewRetryHandler(name string, handler Handler, backoffs []time.Duration) *RetryHandler {
	if len(backoffs) == 0 {
		Log.Warn("NewRetryHandler() len(backoffs) == 0")
	}
	return &RetryHandler{
		Name:     name,
		Log:      Log.New("mixin", "handler_retry", "name", name),
		handler:  handler,
		backoffs: backoffs,
	}
}

func (r *RetryHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	bk := r.backoffs
	to_wait := 0 * time.Second
	for {
		r.Log.Debug("[Handler] Handle() ...","msg_in", msg.Id(),
			"count", len(r.backoffs)-len(bk)+1, "wait", to_wait)
		// A negative or zero duration causes Sleep to return immediately.
		time.Sleep(to_wait)
		// assert end_allowed.Sub(now) != 0
		msg_out, err := r.handler.Handle(msg)
		timespan := time.Now().Sub(begin).Seconds()
		if err == nil {
			r.Log.Debug("[Handler] Handle() ok", "msg_in", msg.Id(),
				"msg_out", msg_out.Id(), "timespan", timespan)
			return msg_out, err
		}
		if len(bk) == 0 {
			r.Log.Error("[Handler] Handle() err and no more backing off",
				"err", err, "msg_in", msg.Id(),
				"timespan", timespan)
			return nil, err
		} else {
			r.Log.Error("[Handler] Handle() err, backing off ...",
				"err", err, "msg_in", msg.Id(),
				"timespan", timespan)
			to_wait = bk[0]
			bk = bk[1:]
		}
	}
}

// Bulkhead pattern is used to limit the number of concurrent Handle().
type BulkheadHandler struct {
	Name      string
	Log       log15.Logger
	handler   Handler
	semaphore chan *struct{}
}

// Create a BulkheadHandler that allows at maximum $max_concurrency Handle() to
// run concurrently.
func NewBulkheadHandler(name string, handler Handler, max_concurrency int) *BulkheadHandler {
	if max_concurrency <= 0 {
		panic(errors.New("max_concurrent must be greater than 0"))
	}
	return &BulkheadHandler{
		Name:      name,
		Log:       Log.New("mixin", "handler_bulk", "name", name),
		handler:   handler,
		semaphore: make(chan *struct{}, max_concurrency),
	}
}

// Handle() is blocked when the limit is exceeded.
func (r *BulkheadHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Handler] Handle() ...", "msg_in", msg.Id())
	r.semaphore <- &struct{}{}
	msg_out, err := r.handler.Handle(msg)
	<-r.semaphore
	// timespan covers <-r.semaphore
	timespan := time.Now().Sub(begin).Seconds()
	if err != nil {
		r.Log.Error("[Handler] Handle() err", "msg_in", msg.Id(),
			"err", err, "timespan", timespan)
		return nil, err
	}
	r.Log.Debug("[Handler] Handle() ok", "msg_in", msg.Id(),
		"msg_out", msg_out.Id(), "timespan", timespan)
	return msg_out, err
}

// CircuitBreakerHandler is a handler equipped with a circuit-breaker.
type CircuitBreakerHandler struct {
	Name    string
	Log     log15.Logger
	Circuit string
	handler Handler
}

// In hystrix-go, a circuit-breaker must be given a unique name.
// NewCircuitBreakerHandler() creates a CircuitBreakerHandler with a
// circuit-breaker named $circuit.
func NewCircuitBreakerHandler(name string, handler Handler, circuit string) *CircuitBreakerHandler {
	return &CircuitBreakerHandler{
		Name: name,
		Log: Log.New("mixin", "handler_circuit", "name", name,
			"circuit", circuit),
		Circuit: circuit,
		handler: handler,
	}
}

func (r *CircuitBreakerHandler) Handle(msg Message) (Message, error) {
	r.Log.Debug("[Handler] Handle() ...", "msg_in", msg.Id())
	result := make(chan *tuple, 1)
	err := hystrix.Do(r.Circuit, func() error {
		msg_out, err := r.handler.Handle(msg)
		if err != nil {
			r.Log.Error("[Handler] Handle() err", "msg_in", msg.Id(), "err", err)
			result <- &tuple{
				fst: msg_out,
				snd: err,
			}
			return err
		}
		r.Log.Debug("[Handler] Handle() ok", "msg_in", msg.Id(), "msg_out", msg_out.Id())
		result <- &tuple{
			fst: msg_out,
			snd: err,
		}
		return nil
	}, nil)
	// hystrix.ErrTimeout doesn't interrupt work anyway.
	// It just contributes to circuit's metrics.
	if err != nil {
		r.Log.Warn("[Handler] Circuit err", "msg_in", msg.Id(), "err", err)
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

