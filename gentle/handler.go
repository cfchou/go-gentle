package gentle

import (
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/benbjohnson/clock"
	"sync"
	"time"
)

const (
	// Handler types(mixins), are most often used as part of RegistryKey.
	MIXIN_HANDLER_RATELIMITED    = "hRate"
	MIXIN_HANDLER_RETRY          = "hRetry"
	MIXIN_HANDLER_BULKHEAD       = "hBulk"
	MIXIN_HANDLER_SEMAPHORE      = "hSem"
	MIXIN_HANDLER_CIRCUITBREAKER = "hCircuit"
	MIXIN_HANDLER_HANDLED        = "hHan"
	MIXIN_HANDLER_FALLBACK       = "hFb"
)

// Common options for XXXHandlerOpts
type handlerOpts struct {
	Namespace    string
	Name         string
	Log          Logger
	MetricHandle Metric
}

// Common fields for XXXHandler
type handlerFields struct {
	namespace string
	name      string
	log       Logger
	mxHandle  Metric
}

func newHandlerFields(opts *handlerOpts) *handlerFields {
	return &handlerFields{
		namespace: opts.Namespace,
		name:      opts.Name,
		log:       opts.Log,
		mxHandle:  opts.MetricHandle,
	}
}

type RateLimitedHandlerOpts struct {
	handlerOpts
	Limiter RateLimit
}

func NewRateLimitedHandlerOpts(namespace, name string, limiter RateLimit) *RateLimitedHandlerOpts {
	return &RateLimitedHandlerOpts{
		handlerOpts: handlerOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"mixin", MIXIN_HANDLER_RATELIMITED, "name", name),
			MetricHandle: noopMetric,
		},
		Limiter: limiter,
	}
}

// Rate limiting pattern is used to limit the speed of a series of Handle().
type rateLimitedHandler struct {
	*handlerFields
	limiter RateLimit
	handler Handler
}

func NewRateLimitedHandler(opts *RateLimitedHandlerOpts, handler Handler) *rateLimitedHandler {
	return &rateLimitedHandler{
		handlerFields: newHandlerFields(&opts.handlerOpts),
		limiter:       opts.Limiter,
		handler:       handler,
	}
}

// Handle() is blocked when the limit is exceeded.
func (r *rateLimitedHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	r.log.Debug("[Handler] Handle() ...", "msg_in", msg.Id())
	r.limiter.Wait(1, 0)
	msg_out, err := r.handler.Handle(msg)
	timespan := time.Since(begin).Seconds()
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

func (r *rateLimitedHandler) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin:     MIXIN_HANDLER_RATELIMITED,
		Name:      r.name,
	}
}

type RetryHandlerOpts struct {
	handlerOpts
	MetricTryNum   Metric
	Clock          Clock
	BackOffFactory BackOffFactory
}

func NewRetryHandlerOpts(namespace, name string, backOffFactory BackOffFactory) *RetryHandlerOpts {
	return &RetryHandlerOpts{
		handlerOpts: handlerOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace, "mixin",
				MIXIN_HANDLER_RETRY, "name", name),
			MetricHandle: noopMetric,
		},
		MetricTryNum:   noopMetric,
		Clock:          clock.New(),
		BackOffFactory: backOffFactory,
	}
}

// retryHandler takes an Handler. When Handler.Handle() encounters an error,
// retryHandler back off for some time and then retries.
type retryHandler struct {
	*handlerFields
	mxTryNum       Metric
	clock          Clock
	backOffFactory BackOffFactory
	handler        Handler
}

func NewRetryHandler(opts *RetryHandlerOpts, handler Handler) *retryHandler {
	return &retryHandler{
		handlerFields:  newHandlerFields(&opts.handlerOpts),
		mxTryNum:       opts.MetricTryNum,
		clock:          opts.Clock,
		backOffFactory: opts.BackOffFactory,
		handler:        handler,
	}
}

func (r *retryHandler) Handle(msg Message) (Message, error) {
	begin := r.clock.Now()
	count := 1
	r.log.Debug("[Handler] Handle() ...", "count", count)
	var once sync.Once
	var backOff BackOff
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
		once.Do(func() {
			backOff = r.backOffFactory.NewBackOff()
		})
		to_wait := backOff.Next()
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
		// passed as "elapsed".
		count++
		r.log.Error("[Handler] Handle() err, backing off ...",
			"err", err, "msg_in", msg.Id(), "elapsed", timespan,
			"count", count, "wait", to_wait)
		r.clock.Sleep(to_wait)
	}
}

func (r *retryHandler) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin:     MIXIN_HANDLER_RETRY,
		Name:      r.name,
	}
}

type BulkheadHandlerOpts struct {
	handlerOpts
	MaxConcurrency int
}

func NewBulkheadHandlerOpts(namespace, name string, maxConcurrency int) *BulkheadHandlerOpts {
	if maxConcurrency <= 0 {
		panic(errors.New("maxConcurrent must be greater than 0"))
	}
	return &BulkheadHandlerOpts{
		handlerOpts: handlerOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"mixin", MIXIN_HANDLER_BULKHEAD, "name", name),
			MetricHandle: noopMetric,
		},
		MaxConcurrency: maxConcurrency,
	}
}

// Bulkhead pattern is used to limit the number of concurrently hanging Handle().
// It uses semaphore isolation, similar to the approach used in hystrix.
// http://stackoverflow.com/questions/30391809/what-is-bulkhead-pattern-used-by-hystrix
type bulkheadHandler struct {
	*handlerFields
	handler   Handler
	semaphore chan struct{}
}

// Create a bulkheadHandler that allows at maximum $max_concurrency Handle() to
// run concurrently.
func NewBulkheadHandler(opts *BulkheadHandlerOpts, handler Handler) *bulkheadHandler {

	return &bulkheadHandler{
		handlerFields: newHandlerFields(&opts.handlerOpts),
		handler:       handler,
		semaphore:     make(chan struct{}, opts.MaxConcurrency),
	}
}

// Handle() is blocked when the limit is exceeded.
func (r *bulkheadHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	r.log.Debug("[Handler] Handle() ...", "msg_in", msg.Id())
	select {
	case r.semaphore <- struct{}{}:
		defer func() {
			<-r.semaphore
		}()
		msg_out, err := r.handler.Handle(msg)
		timespan := time.Since(begin).Seconds()
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
	default:
		r.log.Error("[Hander] Handle() err", "msg_in", msg.Id(),
			"err", ErrMaxConcurrency)
		return nil, ErrMaxConcurrency
	}
}

func (r *bulkheadHandler) GetMaxConcurrency() int {
	return cap(r.semaphore)
}

func (r *bulkheadHandler) GetCurrentConcurrency() int {
	return len(r.semaphore)
}

func (r *bulkheadHandler) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin:     MIXIN_HANDLER_BULKHEAD,
		Name:      r.name,
	}
}

type SemaphoreHandlerOpts struct {
	handlerOpts
	MaxConcurrency int
}

func NewSemaphoreHandlerOpts(namespace, name string, maxConcurrency int) *SemaphoreHandlerOpts {
	if maxConcurrency <= 0 {
		panic(errors.New("maxConcurrent must be greater than 0"))
	}
	return &SemaphoreHandlerOpts{
		handlerOpts: handlerOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"mixin", MIXIN_HANDLER_SEMAPHORE, "name", name),
			MetricHandle: noopMetric,
		},
		MaxConcurrency: maxConcurrency,
	}
}

// It allows at maximum $max_concurrency Handle() to run concurrently. Similar
// to Bulkhead, but it blocks when MaxConcurrency is reached.
type semaphoreHandler struct {
	*handlerFields
	handler   Handler
	semaphore chan struct{}
}

func NewSemaphoreHandler(opts *SemaphoreHandlerOpts, handler Handler) *semaphoreHandler {
	return &semaphoreHandler{
		handlerFields: newHandlerFields(&opts.handlerOpts),
		handler:       handler,
		semaphore:     make(chan struct{}, opts.MaxConcurrency),
	}
}

func (r *semaphoreHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	r.log.Debug("[Handler] Handle() ...", "msg_in", msg.Id())
	r.semaphore <- struct{}{}
	defer func() { <-r.semaphore }()
	msg_out, err := r.handler.Handle(msg)
	timespan := time.Since(begin).Seconds()
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

func (r *semaphoreHandler) GetMaxConcurrency() int {
	return cap(r.semaphore)
}

func (r *semaphoreHandler) GetCurrentConcurrency() int {
	return len(r.semaphore)
}

func (r *semaphoreHandler) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin:     MIXIN_HANDLER_SEMAPHORE,
		Name:      r.name,
	}
}

type CircuitBreakerHandlerOpts struct {
	handlerOpts
	MetricCbErr Metric
	Circuit     string
}

func NewCircuitBreakerHandlerOpts(namespace, name, circuit string) *CircuitBreakerHandlerOpts {
	return &CircuitBreakerHandlerOpts{
		handlerOpts: handlerOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"mixin", MIXIN_HANDLER_CIRCUITBREAKER,
				"name", name, "circuit", circuit),
			MetricHandle: noopMetric,
		},
		MetricCbErr: noopMetric,
		Circuit:     circuit,
	}
}

// circuitBreakerHandler is a handler equipped with a circuit-breaker.
type circuitBreakerHandler struct {
	*handlerFields
	mxCbErr Metric
	circuit string
	handler Handler
}

// In hystrix-go, a circuit-breaker must be given a unique name.
// NewCircuitBreakerStream() creates a circuitBreakerStream with a
// circuit-breaker named $circuit.
func NewCircuitBreakerHandler(opts *CircuitBreakerHandlerOpts, handler Handler) *circuitBreakerHandler {
	return &circuitBreakerHandler{
		handlerFields: newHandlerFields(&opts.handlerOpts),
		mxCbErr:       opts.MetricCbErr,
		circuit:       opts.Circuit,
		handler:       handler,
	}
}

func (r *circuitBreakerHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	r.log.Debug("[Handler] Handle() ...", "msg_in", msg.Id())
	result := make(chan interface{}, 1)
	err := hystrix.Do(r.circuit, func() error {
		msg_out, err := r.handler.Handle(msg)
		timespan := time.Since(begin).Seconds()
		if err != nil {
			r.log.Error("[Handler] Do()::Handle() err", "msg_in", msg.Id(),
				"err", err, "timespan", timespan)
			return err
		}
		r.log.Debug("[Handler] Do()::Handle() ok",
			"msg_in", msg.Id(), "msg_out", msg_out.Id(),
			"timespan", timespan)
		result <- msg_out
		return nil
	}, nil)
	// NOTE:
	// err can be from Do()::Handle() or hystrix errors if criteria are matched.
	// Do()::Handle()'s err, being returned or not, contributes to hystrix
	// metrics if !PassCircuitBreaker(err).
	if err != nil {
		defer func() {
			timespan := time.Since(begin).Seconds()
			r.log.Error("[Handler] Circuit err",
				"msg_in", msg.Id(), "err", err,
				"timespan", timespan)
			r.mxHandle.Observe(timespan, label_err)
		}()
		// To prevent misinterpreting when wrapping one
		// circuitBreakerStream over another. Hystrix errors are
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
	msg_out := (<-result).(Message)
	timespan := time.Since(begin).Seconds()
	r.log.Debug("[Handler] Handle() ok", "msg_in", msg.Id(),
		"msg_out", msg_out.Id(), "timespan", timespan)
	r.mxHandle.Observe(timespan, label_ok)
	return msg, nil
}

func (r *circuitBreakerHandler) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin:     MIXIN_HANDLER_CIRCUITBREAKER,
		Name:      r.name,
	}
}

func (r *circuitBreakerHandler) GetCircuitName() string {
	return r.circuit
}

type FallbackHandlerOpts struct {
	handlerOpts
	FallbackFunc func(Message, error) (Message, error)
}

func NewFallbackHandlerOpts(namespace, name string,
	fallbackFunc func(Message, error) (Message, error)) *FallbackHandlerOpts {
	return &FallbackHandlerOpts{
		handlerOpts: handlerOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace,
				"mixin", MIXIN_HANDLER_FALLBACK, "name", name),
			MetricHandle: noopMetric,
		},
		FallbackFunc: fallbackFunc,
	}
}

// fallbackHandler transforms what Handler.Handle() returns.
type fallbackHandler struct {
	*handlerFields
	fallbackFunc func(Message, error) (Message, error)
	handler      Handler
}

func NewFallbackHandler(opts *FallbackHandlerOpts, handler Handler) *fallbackHandler {
	return &fallbackHandler{
		handlerFields: newHandlerFields(&opts.handlerOpts),
		fallbackFunc:  opts.FallbackFunc,
		handler:       handler,
	}
}

func (r *fallbackHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	r.log.Debug("[Handler] Handle() ...", "msg_in", msg.Id())
	msg_out, err := r.handler.Handle(msg)
	if err == nil {
		timespan := time.Since(begin).Seconds()
		r.log.Debug("[Handler] Handle() ok, skip fallbackFunc",
			"msg_in", msg.Id(), "msg_out", msg_out.Id(), timespan)
		r.mxHandle.Observe(timespan, label_ok)
		return msg_out, nil
	}
	r.log.Error("[Handler] Handle() err, fallbackFunc() ...", "err", err)
	// fallback to deal with the err and the msg that caused it.
	msg_out, err = r.fallbackFunc(msg, err)
	timespan := time.Since(begin).Seconds()
	if err != nil {
		r.log.Error("[Handler] fallbackFunc() err",
			"msg_in", msg.Id(), "err", err, "timespan", timespan)
		r.mxHandle.Observe(timespan, label_err)
		return nil, err
	}
	r.log.Debug("[Handler] fallbackFunc() ok",
		"msg_in", msg.Id(), "msg_out", msg_out.Id(),
		"timespan", timespan)
	r.mxHandle.Observe(timespan, label_ok)
	return msg_out, nil
}

func (r *fallbackHandler) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin:     MIXIN_HANDLER_FALLBACK,
		Name:      r.name,
	}
}

type HandlerMappedHandlerOpts struct {
	handlerOpts
}

func NewHandlerMappedHandlerOpts(namespace, name string) *HandlerMappedHandlerOpts {
	return &HandlerMappedHandlerOpts{
		handlerOpts: handlerOpts{
			Namespace: namespace,
			Name:      name,
			Log: Log.New("namespace", namespace, "mixin",
				MIXIN_HANDLER_HANDLED, "name", name),
			MetricHandle: noopMetric,
		},
	}
}

type handlerMappedHandler struct {
	*handlerFields
	prevHandler Handler
	handler     Handler
}

func NewHandlerMappedHandler(opts *HandlerMappedHandlerOpts, prevHandler Handler,
	handler Handler) *handlerMappedHandler {
	return &handlerMappedHandler{
		handlerFields: newHandlerFields(&opts.handlerOpts),
		prevHandler:   prevHandler,
		handler:       handler,
	}
}

func (r *handlerMappedHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	r.log.Debug("[Handler] prev.Handle() ...")
	msg_mid, err := r.prevHandler.Handle(msg)
	if err != nil {
		timespan := time.Since(begin).Seconds()
		r.log.Error("[Handler] prev.Handle() err", "msg_in", msg.Id(),
			"err", err, "timespan", timespan)
		r.mxHandle.Observe(timespan, label_err)
		return nil, err
	}
	r.log.Debug("[Handler] prev.Handle() ok", "msg_in", msg.Id(),
		"msg_mid", msg_mid.Id())
	msg_out, herr := r.handler.Handle(msg)
	timespan := time.Since(begin).Seconds()
	if herr != nil {
		r.log.Error("[Handler] Handle() err", "msg_mid", msg_mid.Id(),
			"err", herr, "timespan", timespan)
		r.mxHandle.Observe(timespan, label_err)
		return nil, herr
	}
	r.log.Debug("[Handler] Handle() ok", "msg_mid", msg_mid.Id(),
		"msg_out", msg_out.Id())
	r.mxHandle.Observe(timespan, label_ok)
	return msg_out, nil

}

type simpleHandler struct {
	handleFunc func(Message) (Message, error)
}

func (r *simpleHandler) Handle(msg Message) (Message, error) {
	return r.handleFunc(msg)
}

// A helper to create a simplest Handler without facilities like logger and
// metrics.
func NewSimpleHandler(handleFunc func(Message) (Message, error)) *simpleHandler {
	return &simpleHandler{
		handleFunc: handleFunc,
	}
}
