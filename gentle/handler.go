package gentle

import (
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	"time"
	"github.com/benbjohnson/clock"
)

const (
	// Handler types(mixins), are most often used as part of RegistryKey.
	MIXIN_HANDLER_RATELIMITED    = "hRate"
	MIXIN_HANDLER_RETRY          = "hRetry"
	MIXIN_HANDLER_BULKHEAD       = "hBulk"
	MIXIN_HANDLER_CIRCUITBREAKER = "hCircuit"
	MIXIN_HANDLER_TRANS          = "hTrans"
)

// Common options for XXXHandlerOpts
type HandlerOpts struct {
	Namespace      string
	Name           string
	Log            Logger
	MetricHandle 	       Metric
}

// Common fields for XXXHandler
type handlerFields struct {
	namespace         string
	name              string
	log               Logger
	mxHandle 	Metric
}

func newHandlerFields(opts *HandlerOpts) *handlerFields {
	return &handlerFields{
		namespace: opts.Namespace,
		name:      opts.Name,
		log:       opts.Log,
		mxHandle:     opts.MetricHandle,
	}
}

type RateLimitedHandlerOpts struct {
	HandlerOpts
	Limiter           RateLimit
}

func NewRateLimitedHandlerOpts(namespace, name string, limiter RateLimit) *RateLimitedHandlerOpts {
	return &RateLimitedHandlerOpts{
		HandlerOpts: HandlerOpts{
			Namespace: namespace,
			Name:name,
			Log: Log.New("namespace", namespace,
				"mixin", MIXIN_HANDLER_RATELIMITED, "name", name),
			MetricHandle: noopMetric,
		},
		Limiter: limiter,
	}
}

// Rate limiting pattern is used to limit the speed of a series of Handle().
type RateLimitedHandler struct {
	handlerFields
	limiter           RateLimit
	handler           Handler
}

func NewRateLimitedHandler(opts RateLimitedHandlerOpts, handler Handler) *RateLimitedHandler {
	return &RateLimitedHandler{
		handlerFields: *newHandlerFields(&opts.HandlerOpts),
		limiter: opts.Limiter,
		handler: handler,
	}
}

// Handle() is blocked when the limit is exceeded.
func (r *RateLimitedHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	r.log.Debug("[Handler] Handle() ...", "msg_in", msg.Id())
	r.limiter.Wait(1, 0)
	msg_out, err := r.handler.Handle(msg)
	timespan := time.Now().Sub(begin).Seconds()
	if err != nil {
		r.log.Error("[Handler] Handle() err", "msg_in", msg.Id(),
			"err", err, "timespan", timespan)
		r.mxHandle.Observe(timespan, label_err)
		return nil, err
	}
	r.log.Debug("[Handler] Handle() ok", "msg_in", msg.Id(),
		"msg_out", msg_out.Id(), "timespan", timespan)
	r.mxHandle.Observe(timespan, label_ok)
	return msg_out, nil
}

func (r *RateLimitedHandler) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin: MIXIN_HANDLER_RATELIMITED,
		Name: r.name,
	}
}

type RetryHandlerOpts struct {
	HandlerOpts
	MetricTryNum       Metric
	Clock     clock.Clock
	BackOff    BackOff
}

func NewRetryHandlerOpts(namespace, name string, backoff BackOff) *RetryHandlerOpts {
	return &RetryHandlerOpts{
		HandlerOpts: HandlerOpts{
			Namespace: namespace,
			Name:name,
			Log: Log.New("namespace", namespace, "mixin",
				MIXIN_HANDLER_RETRY, "name", name),
			MetricHandle: noopMetric,
		},
		MetricTryNum: noopMetric,
		Clock: clock.New(),
		BackOff: backoff,
	}
}

// RetryHandler takes an Handler. When Handler.Handle() encounters an error,
// RetryHandler back off for some time and then retries.
type RetryHandler struct {
	handlerFields
	mxTryNum    Metric
	clock clock.Clock
	backOff    BackOff
	handler           Handler
}

func NewRetryHandler(opts RetryHandlerOpts, handler Handler) *RetryHandler {
	return &RetryHandler{
		handlerFields: *newHandlerFields(&opts.HandlerOpts),
		mxTryNum: opts.MetricTryNum,
		clock: opts.Clock,
		backOff: opts.BackOff,
		handler:  handler,
	}
}

func (r *RetryHandler) Handle(msg Message) (Message, error) {
	begin := r.clock.Now()
	count := 1
	r.log.Debug("[Handler] Handle() ...", "count", count)
	for {
		msg_out, err := r.handler.Handle(msg)
		if err == nil {
			timespan := r.clock.Now().Sub(begin).Seconds()
			r.log.Debug("[Handler] Handle() ok", "msg_in", msg.Id(),
				"msg_out", msg_out.Id(), "timespan", timespan)
			r.mxHandle.Observe(timespan, label_ok)
			r.mxTryNum.Observe(float64(count), label_ok)
			return msg_out, nil
		}
		to_wait := r.backOff.Next()
		// Next() should immediately return but we can't guarantee so
		// timespan is calculated after Next().
		timespan := r.clock.Now().Sub(begin).Seconds()
		if to_wait == BackOffStop {
			r.log.Error("[Handler] Handle() err and no more backing off",
				"msg_in", msg.Id(), "err", err,
				"timespan", timespan)
			r.mxHandle.Observe(timespan, label_err)
			r.mxTryNum.Observe(float64(count), label_err)
			return nil, err
		}
		// timespan in our convention is used to track the overall
		// time of current function. Here we record time
		// passed as "elapse".
		count++
		r.log.Error("[Handler] Handle() err, backing off ...",
			"err", err, "msg_in", msg.Id(), "elapse", timespan,
			"count", count, "wait", to_wait)
		r.clock.Sleep(to_wait)
	}
}

func (r *RetryHandler) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin: MIXIN_HANDLER_RETRY,
		Name: r.name,
	}
}

type BulkheadHandlerOpts struct {
	HandlerOpts
	MaxConcurrency int
}

func NewBulkheadHandlerOpts(namespace, name string, max_concurrency int) *BulkheadHandlerOpts {
	if max_concurrency <= 0 {
		panic(errors.New("max_concurrent must be greater than 0"))
	}
	return &BulkheadHandlerOpts{
		HandlerOpts: HandlerOpts {
			Namespace: namespace,
			Name:name,
			Log: Log.New("namespace", namespace,
				"mixin", MIXIN_HANDLER_BULKHEAD, "name", name),
			MetricHandle: noopMetric,
		},
		MaxConcurrency: max_concurrency,
	}
}

// Bulkhead pattern is used to limit the number of concurrently hanging Handle().
// It uses semaphore isolation, similar to the approach used in hystrix.
// http://stackoverflow.com/questions/30391809/what-is-bulkhead-pattern-used-by-hystrix
type BulkheadHandler struct {
	handlerFields
	handler           Handler
	semaphore         chan *struct{}
}

// Create a BulkheadHandler that allows at maximum $max_concurrency Handle() to
// run concurrently.
func NewBulkheadHandler(opts BulkheadHandlerOpts, handler Handler) *BulkheadHandler {

	return &BulkheadHandler{
		handlerFields: *newHandlerFields(&opts.HandlerOpts),
		handler:   handler,
		semaphore: make(chan *struct{}, opts.MaxConcurrency),
	}
}

// Handle() is blocked when the limit is exceeded.
func (r *BulkheadHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	r.log.Debug("[Handler] Handle() ...", "msg_in", msg.Id())
	select {
	case r.semaphore <- &struct{}{}:
		defer func() {
			<-r.semaphore
		}()
		msg_out, err := r.handler.Handle(msg)
		timespan := time.Now().Sub(begin).Seconds()
		if err != nil {
			r.log.Error("[Handler] Handle() err", "msg_in", msg.Id(),
				"err", err, "timespan", timespan)
			r.mxHandle.Observe(timespan, label_err)
			return nil, err
		}
		r.log.Debug("[Handler] Handle() ok","msg_in", msg.Id(),
			"msg_out", msg_out.Id(), "timespan", timespan)
		r.mxHandle.Observe(timespan, label_ok)
		return msg_out, nil
	default:
		r.log.Error("[Hander] Handle() err","msg_in", msg.Id(),
			"err", ErrMaxConcurrency)
		return nil, ErrMaxConcurrency
	}
}

func (r *BulkheadHandler) GetMaxConcurrency() int {
	return cap(r.semaphore)
}

func (r *BulkheadHandler) GetCurrentConcurrency() int {
	return len(r.semaphore)
}

type CircuitBreakerHandlerOpts struct {
	HandlerOpts
	MetricCbErr     Metric
	Circuit   string
}

func NewCircuitBreakerHandlerOpts(namespace, name, circuit string) *CircuitBreakerHandlerOpts {
	return &CircuitBreakerHandlerOpts{
		HandlerOpts: HandlerOpts{
			Namespace: namespace,
			Name:name,
			Log: Log.New("namespace", namespace,
				"mixin", MIXIN_HANDLER_CIRCUITBREAKER,
				"name", name, "circuit", circuit),
			MetricHandle: noopMetric,
		},
		MetricCbErr: noopMetric,
		Circuit: circuit,
	}
}

// CircuitBreakerHandler is a handler equipped with a circuit-breaker.
type CircuitBreakerHandler struct {
	handlerFields
	mxCbErr Metric
	circuit           string
	handler           Handler
}

// In hystrix-go, a circuit-breaker must be given a unique name.
// NewCircuitBreakerStream() creates a CircuitBreakerStream with a
// circuit-breaker named $circuit.
func NewCircuitBreakerHandler(opts CircuitBreakerHandlerOpts, handler Handler) *CircuitBreakerHandler {
	return &CircuitBreakerHandler{
		handlerFields: *newHandlerFields(&opts.HandlerOpts),
		mxCbErr: opts.MetricCbErr,
		circuit: opts.Circuit,
		handler: handler,
	}
}

func (r *CircuitBreakerHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	r.log.Debug("[Handler] Handle() ...", "msg_in", msg.Id())
	result := make(chan Message, 1)
	err := hystrix.Do(r.circuit, func() error {
		msg_out, err := r.handler.Handle(msg)
		timespan := time.Now().Sub(begin).Seconds()
		if err != nil {
			r.log.Error("[Handler] Handle() in CB err",
				"msg_in", msg.Id(),
				"err", err, "timespan", timespan)
			return err
		}
		r.log.Debug("[Handler] Handle() in CB ok",
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
			r.log.Error("[Handler] Circuit err",
				"msg_in", msg.Id(), "err", err,
				"timespan", timespan)
			r.mxHandle.Observe(timespan, label_err)
		}()
		// To prevent misinterpreting when wrapping one
		// CircuitBreakerStream over another. Hystrix errors are
		// replaced so that Get() won't return any hystrix errors.
		switch err {
		case hystrix.ErrCircuitOpen:
			r.mxCbErr.Observe(1,
				map[string]string{"err": "ErrCbOpen"})
			return nil, ErrCbOpen
		case hystrix.ErrMaxConcurrency:
			r.mxCbErr.Observe(1,
				map[string]string{"err": "ErrCbMaxConcurrency"})
			return nil, ErrCbMaxConcurrency
		case hystrix.ErrTimeout:
			r.mxCbErr.Observe(1,
				map[string]string{"err": "ErrCbTimeout"})
			return nil, ErrCbTimeout
		default:
			r.mxCbErr.Observe(1,
				map[string]string{"err": "NonCbErr"})
			return nil, err
		}
	}
	msg_out := <-result
	timespan := time.Now().Sub(begin).Seconds()
	r.log.Debug("[Handler] Handle() ok", "msg_in", msg.Id(),
		"msg_out", msg_out.Id(), "timespan", timespan)
	r.mxHandle.Observe(timespan, label_ok)
	return msg, nil
}

func (r *CircuitBreakerHandler) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin: MIXIN_HANDLER_CIRCUITBREAKER,
		Name: r.name,
	}
}

func (r *CircuitBreakerHandler) GetCircuitName() string {
	return r.circuit
}

type TransformHandlerOpts struct {
	HandlerOpts
	TransFunc      func(Message, error) (Message, error)
}

func NewTransformHandlerOpts(namespace, name string,
	transFunc func(Message, error) (Message, error)) *TransformHandlerOpts {
	return &TransformHandlerOpts{
		HandlerOpts: HandlerOpts{
			Namespace: namespace,
			Name:name,
			Log: Log.New("namespace", namespace,
				"mixin", MIXIN_HANDLER_TRANS, "name", name),
			MetricHandle: noopMetric,
		},
		TransFunc: transFunc,
	}
}

// TransformHandler transforms what Handler.Handle() returns.
type TransformHandler struct {
	handlerFields
	transFunc         func(Message, error) (Message, error)
	handler           Handler
}

func NewTransformHandler(opts TransformHandlerOpts, handler Handler) *TransformHandler {
	return &TransformHandler{
		handlerFields: *newHandlerFields(&opts.HandlerOpts),
		transFunc: opts.TransFunc,
		handler:   handler,
	}
}

func (r *TransformHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	r.log.Debug("[Handler] Handle() ...", "msg_in", msg.Id())
	msg_mid, err := r.handler.Handle(msg)
	if err != nil {
		r.log.Debug("[Handler] Handle() err, transFunc() ...",
			"msg_in", msg.Id(), "err", err)
		// enforce the exclusivity
		msg_mid = nil
	} else {
		r.log.Debug("[Handler] Handle() ok, transFunc() ...",
			"msg_in", msg.Id(), "msg_mid", msg_mid.Id())
	}
	msg_out, err2 := r.transFunc(msg_mid, err)
	timespan := time.Now().Sub(begin).Seconds()
	if err2 != nil {
		if msg_mid != nil {
			r.log.Error("[Handler] transFunc() err",
				"msg_in", msg.Id(), "msg_mid", msg_mid.Id(),
				"err", err2, "timespan", timespan)
		} else {
			r.log.Error("[Handler] transFunc() err",
				"msg_in", msg.Id(), "err", err2,
				"timespan", timespan)
		}
		r.mxHandle.Observe(timespan, label_err)
		return nil, err2
	}
	if msg_mid != nil {
		r.log.Debug("[Handler] transFunc() ok",
			"msg_in", msg.Id(), "msg_mid", msg_mid.Id(),
			"msg_out", msg_out.Id(), "timespan", timespan)
	} else {
		r.log.Debug("[Handler] transFunc() ok",
			"msg_in", msg.Id(), "msg_out", msg_out.Id(),
			"timespan", timespan)
	}
	r.mxHandle.Observe(timespan, label_ok)
	return msg_out, nil
}

func (r *TransformHandler) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin: MIXIN_HANDLER_TRANS,
		Name: r.name,
	}
}
