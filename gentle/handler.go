package gentle

import (
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	log15 "gopkg.in/inconshreveable/log15.v2"
	"time"
)

const (
	// Handler types(mixins), are most often used as part of RegistryKey.
	MIXIN_HANDLER_RATELIMITED    = "hRate"
	MIXIN_HANDLER_RETRY          = "hRetry"
	MIXIN_HANDLER_BULKHEAD       = "hBulk"
	MIXIN_HANDLER_CIRCUITBREAKER = "hCircuit"
	MIXIN_HANDLER_TRANS          = "hTrans"
)

// Rate limiting pattern is used to limit the speed of a series of Handle().
type RateLimitedHandler struct {
	Namespace         string
	Name              string
	Log               log15.Logger
	handler           Handler
	limiter           RateLimit
	handleObservation Observation
}

func NewRateLimitedHandler(namespace, name string, handler Handler,
	limiter RateLimit) *RateLimitedHandler {

	return &RateLimitedHandler{
		Namespace: namespace,
		Name:      name,
		Log: Log.New("namespace", namespace,
			"mixin", MIXIN_HANDLER_RATELIMITED, "name", name),
		handler: handler,
		limiter: limiter,
		handleObservation: NoOpObservationIfNonRegistered(
			&RegistryKey{namespace,
				MIXIN_HANDLER_RATELIMITED,
				name, MX_HANDLER_HANDLE}),
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
		r.handleObservation.Observe(timespan, label_err)
		return nil, err
	}
	r.Log.Debug("[Handler] Handle() ok", "msg_in", msg.Id(),
		"msg_out", msg_out.Id(), "timespan", timespan)
	r.handleObservation.Observe(timespan, label_ok)
	return msg_out, nil
}

// RetryHandler takes an Handler. When Handler.Handle() encounters an error,
// RetryHandler back off for some time and then retries.
type RetryHandler struct {
	Namespace         string
	Name              string
	Log               log15.Logger
	handler           Handler
	backoffs          []time.Duration
	handleObservation Observation
	tryObservation    Observation
}

func NewRetryHandler(namespace, name string, handler Handler,
	backoffs []time.Duration) *RetryHandler {

	if len(backoffs) == 0 {
		Log.Warn("NewRetryHandler() len(backoffs) == 0")
	}
	return &RetryHandler{
		Namespace: namespace,
		Name:      name,
		Log: Log.New("namespace", namespace, "mixin",
			MIXIN_HANDLER_RETRY, "name", name),
		handler:  handler,
		backoffs: backoffs,
		handleObservation: NoOpObservationIfNonRegistered(
			&RegistryKey{namespace,
				MIXIN_HANDLER_RETRY,
				name, MX_HANDLER_HANDLE}),
		tryObservation: NoOpObservationIfNonRegistered(
			&RegistryKey{namespace,
				MIXIN_HANDLER_RETRY,
				name, MX_HANDLER_RETRY_TRY}),
	}
}

func (r *RetryHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	bk := r.backoffs
	to_wait := 0 * time.Second
	for {
		count := len(r.backoffs) - len(bk) + 1
		r.Log.Debug("[Handler] Handle() ...", "msg_in", msg.Id(),
			"count", count, "wait", to_wait)
		time.Sleep(to_wait)
		msg_out, err := r.handler.Handle(msg)
		timespan := time.Now().Sub(begin).Seconds()
		if err == nil {
			r.Log.Debug("[Handler] Handle() ok", "msg_in", msg.Id(),
				"msg_out", msg_out.Id(), "timespan", timespan)
			r.handleObservation.Observe(timespan, label_ok)
			r.tryObservation.Observe(float64(count), label_ok)
			return msg_out, nil
		}
		if len(bk) == 0 {
			r.Log.Error("[Handler] Handle() err and no more backing off",
				"msg_in", msg.Id(), "err", err,
				"timespan", timespan)
			r.handleObservation.Observe(timespan, label_err)
			r.tryObservation.Observe(float64(count), label_err)
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
	Namespace         string
	Name              string
	Log               log15.Logger
	handler           Handler
	semaphore         chan *struct{}
	handleObservation Observation
}

// Create a BulkheadHandler that allows at maximum $max_concurrency Handle() to
// run concurrently.
func NewBulkheadHandler(namespace, name string, handler Handler,
	max_concurrency int) *BulkheadHandler {

	if max_concurrency <= 0 {
		panic(errors.New("max_concurrent must be greater than 0"))
	}
	return &BulkheadHandler{
		Namespace: namespace,
		Name:      name,
		Log: Log.New("namespace", namespace,
			"mixin", MIXIN_HANDLER_BULKHEAD, "name", name),
		handler:   handler,
		semaphore: make(chan *struct{}, max_concurrency),
		handleObservation: NoOpObservationIfNonRegistered(
			&RegistryKey{namespace,
				MIXIN_HANDLER_BULKHEAD,
				name, MX_HANDLER_HANDLE}),
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
		r.handleObservation.Observe(timespan, label_err)
		return nil, err
	}
	r.Log.Debug("[Handler] Handle() ok", "msg_in", msg.Id(),
		"msg_out", msg_out.Id(), "timespan", timespan)
	r.handleObservation.Observe(timespan, label_ok)
	return msg_out, nil
}

// CircuitBreakerHandler is a handler equipped with a circuit-breaker.
type CircuitBreakerHandler struct {
	Namespace         string
	Name              string
	Log               log15.Logger
	Circuit           string
	handler           Handler
	handleObservation Observation
	errCounter        Observation
}

// In hystrix-go, a circuit-breaker must be given a unique name.
// NewCircuitBreakerHandler() creates a CircuitBreakerHandler with a
// circuit-breaker named $circuit.
func NewCircuitBreakerHandler(namespace, name string, handler Handler,
	circuit string) *CircuitBreakerHandler {

	return &CircuitBreakerHandler{
		Namespace: namespace,
		Name:      name,
		Log: Log.New("namespace", namespace,
			"mixin", MIXIN_HANDLER_CIRCUITBREAKER, "name", name,
			"circuit", circuit),
		Circuit: circuit,
		handler: handler,
		handleObservation: NoOpObservationIfNonRegistered(
			&RegistryKey{namespace,
				MIXIN_HANDLER_CIRCUITBREAKER,
				name, MX_HANDLER_HANDLE}),
		errCounter: NoOpObservationIfNonRegistered(
			&RegistryKey{namespace,
				MIXIN_HANDLER_CIRCUITBREAKER,
				name,
				MX_HANDLER_CIRCUITBREAKER_HXERR}),
	}
}

func (r *CircuitBreakerHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Handler] Handle() ...", "msg_in", msg.Id())
	result := make(chan Message, 1)
	err := hystrix.Do(r.Circuit, func() error {
		msg_out, err := r.handler.Handle(msg)
		timespan := time.Now().Sub(begin).Seconds()
		if err != nil {
			r.Log.Error("[Handler] Handle() in CB err",
				"msg_in", msg.Id(),
				"err", err, "timespan", timespan)
			return err
		}
		r.Log.Debug("[Handler] Handle() in CB ok",
			"msg_in", msg.Id(), "msg_out", msg_out.Id(),
			"timespan", timespan)
		result <- msg_out
		return nil
	}, nil)
	// hystrix.ErrTimeout doesn't interrupt work anyway.
	// It just contributes to circuit's metrics.
	if err != nil {
		defer func() {
			timespan := time.Now().Sub(begin).Seconds()
			r.Log.Error("[Handler] Circuit err",
				"msg_in", msg.Id(), "err", err,
				"timespan", timespan)
			r.handleObservation.Observe(timespan, label_err)
		}()
		// To prevent misinterpreting when wrapping one
		// CircuitBreakerStream over another. Hystrix errors are
		// replaced so that Get() won't return any hystrix errors.
		switch err {
		case hystrix.ErrCircuitOpen:
			r.errCounter.Observe(1,
				map[string]string{"err": "ErrCircuitOpen"})
			return nil, ErrCircuitOpen
		case hystrix.ErrMaxConcurrency:
			r.errCounter.Observe(1,
				map[string]string{"err": "ErrMaxConcurrency"})
			return nil, ErrMaxConcurrency
		case hystrix.ErrTimeout:
			r.errCounter.Observe(1,
				map[string]string{"err": "ErrTimeout"})
			return nil, ErrTimeout
		default:
			r.errCounter.Observe(1,
				map[string]string{"err": "NonHystrixErr"})
			return nil, err
		}
	}
	msg_out := <-result
	timespan := time.Now().Sub(begin).Seconds()
	r.Log.Debug("[Handler] Handle() ok", "msg_in", msg.Id(),
		"msg_out", msg_out.Id(), "timespan", timespan)
	r.handleObservation.Observe(timespan, label_ok)
	return msg, nil
}

// TransformHandler transforms what Handler.Handle() returns.
type TransformHandler struct {
	Namespace         string
	Name              string
	Log               log15.Logger
	handler           Handler
	transFunc         func(Message, error) (Message, error)
	handleObservation Observation
}

func NewTransformHandler(namespace string, name string, handler Handler,
	transFunc func(Message, error) (Message, error)) *TransformHandler {
	return &TransformHandler{
		Namespace: namespace,
		Name:      name,
		Log: Log.New("namespace", namespace,
			"mixin", MIXIN_HANDLER_TRANS, "name", name),
		handler:   handler,
		transFunc: transFunc,
		handleObservation: NoOpObservationIfNonRegistered(
			&RegistryKey{namespace,
				MIXIN_HANDLER_TRANS,
				name, MX_HANDLER_HANDLE}),
	}
}

func (r *TransformHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Handler] Handle() ...", "msg_in", msg.Id())
	msg_mid, err := r.handler.Handle(msg)
	if err != nil {
		r.Log.Debug("[Handler] Handle() err, transFunc() ...",
			"msg_in", msg.Id(), "err", err)
		// enforce the exclusivity
		msg_mid = nil
	} else {
		r.Log.Debug("[Handler] Handle() ok, transFunc() ...",
			"msg_in", msg.Id(), "msg_mid", msg_mid.Id())
	}
	msg_out, err2 := r.transFunc(msg_mid, err)
	timespan := time.Now().Sub(begin).Seconds()
	if err2 != nil {
		if msg_mid != nil {
			r.Log.Error("[Handler] transFunc() err",
				"msg_in", msg.Id(), "msg_mid", msg_mid.Id(),
				"err", err2, "timespan", timespan)
		} else {
			r.Log.Error("[Handler] transFunc() err",
				"msg_in", msg.Id(), "err", err2,
				"timespan", timespan)
		}
		r.handleObservation.Observe(timespan, label_err)
		return nil, err2
	}
	if msg_mid != nil {
		r.Log.Debug("[Handler] transFunc() ok",
			"msg_in", msg.Id(), "msg_mid", msg_mid.Id(),
			"msg_out", msg_out.Id(), "timespan", timespan)
	} else {
		r.Log.Debug("[Handler] transFunc() ok",
			"msg_in", msg.Id(), "msg_out", msg_out.Id(),
			"timespan", timespan)
	}
	r.handleObservation.Observe(timespan, label_ok)
	return msg_out, nil
}
