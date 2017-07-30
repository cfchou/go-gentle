package gentle

import (
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/benbjohnson/clock"
	"sync"
	"time"
)

const (
	// Types of resilience, are most often used as part of RegistryKey.
	HandlerRateLimited    = "hRate"
	HandlerRetry          = "hRetry"
	HandlerBulkhead       = "hBulk"
	HandlerSemaphore      = "hSem"
	HandlerCircuitBreaker = "hCircuit"
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
				"gentle", HandlerRateLimited, "name", name),
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

func NewRateLimitedHandler(opts *RateLimitedHandlerOpts, handler Handler) Handler {
	return &rateLimitedHandler{
		handlerFields: newHandlerFields(&opts.handlerOpts),
		limiter:       opts.Limiter,
		handler:       handler,
	}
}

// Handle() is blocked when the limit is exceeded.
func (r *rateLimitedHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	r.log.Debug("[Handler] Handle() ...", "msgIn", msg.ID())
	r.limiter.Wait(1, 0)
	msgOut, err := r.handler.Handle(msg)
	timespan := time.Since(begin).Seconds()
	if err != nil {
		r.log.Error("[Handler] Handle() err", "msgIn", msg.ID(),
			"err", err, "timespan", timespan)
		r.mxHandle.Observe(timespan, labelErr)
		return nil, err
	}
	r.log.Debug("[Handler] Handle() ok", "msgIn", msg.ID(),
		"msgOut", msgOut.ID(), "timespan", timespan)
	r.mxHandle.Observe(timespan, labelOk)
	return msgOut, nil
}

func (r *rateLimitedHandler) GetNames() *Names {
	return &Names{
		Namespace:  r.namespace,
		Resilience: HandlerRateLimited,
		Name:       r.name,
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
			Log: Log.New("namespace", namespace, "gentle",
				HandlerRetry, "name", name),
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

func NewRetryHandler(opts *RetryHandlerOpts, handler Handler) Handler {
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
		msgOut, err := r.handler.Handle(msg)
		if err == nil {
			timespan := r.clock.Now().Sub(begin).Seconds()
			r.log.Debug("[Handler] Handle() ok", "msgIn", msg.ID(),
				"msgOut", msgOut.ID(), "timespan", timespan)
			r.mxHandle.Observe(timespan, labelOk)
			r.mxTryNum.Observe(float64(count), labelOk)
			return msgOut, nil
		}
		once.Do(func() {
			backOff = r.backOffFactory.NewBackOff()
		})
		toWait := backOff.Next()
		// Next() should immediately return but we can't guarantee so
		// timespan is calculated after Next().
		timespan := r.clock.Now().Sub(begin).Seconds()
		if toWait == BackOffStop {
			r.log.Error("[Handler] Handle() err and no more backing off",
				"msgIn", msg.ID(), "err", err,
				"timespan", timespan)
			r.mxHandle.Observe(timespan, labelErr)
			r.mxTryNum.Observe(float64(count), labelErr)
			return nil, err
		}
		// timespan in our convention is used to track the overall
		// time of current function. Here we record time
		// passed as "elapsed".
		count++
		r.log.Error("[Handler] Handle() err, backing off ...",
			"err", err, "msgIn", msg.ID(), "elapsed", timespan,
			"count", count, "wait", toWait)
		r.clock.Sleep(toWait)
	}
}

func (r *retryHandler) GetNames() *Names {
	return &Names{
		Namespace:  r.namespace,
		Resilience: HandlerRetry,
		Name:       r.name,
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
				"gentle", HandlerBulkhead, "name", name),
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
func NewBulkheadHandler(opts *BulkheadHandlerOpts, handler Handler) Handler {

	return &bulkheadHandler{
		handlerFields: newHandlerFields(&opts.handlerOpts),
		handler:       handler,
		semaphore:     make(chan struct{}, opts.MaxConcurrency),
	}
}

// Handle() is blocked when the limit is exceeded.
func (r *bulkheadHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	r.log.Debug("[Handler] Handle() ...", "msgIn", msg.ID())
	select {
	case r.semaphore <- struct{}{}:
		defer func() {
			<-r.semaphore
		}()
		msgOut, err := r.handler.Handle(msg)
		timespan := time.Since(begin).Seconds()
		if err != nil {
			r.log.Error("[Handler] Handle() err", "msgIn", msg.ID(),
				"err", err, "timespan", timespan)
			r.mxHandle.Observe(timespan, labelErr)
			return nil, err
		}
		r.log.Debug("[Handler] Handle() ok", "msgIn", msg.ID(),
			"msgOut", msgOut.ID(), "timespan", timespan)
		r.mxHandle.Observe(timespan, labelOk)
		return msgOut, nil
	default:
		r.log.Error("[Hander] Handle() err", "msgIn", msg.ID(),
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
		Namespace:  r.namespace,
		Resilience: HandlerBulkhead,
		Name:       r.name,
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
				"gentle", HandlerSemaphore, "name", name),
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

func NewSemaphoreHandler(opts *SemaphoreHandlerOpts, handler Handler) Handler {
	return &semaphoreHandler{
		handlerFields: newHandlerFields(&opts.handlerOpts),
		handler:       handler,
		semaphore:     make(chan struct{}, opts.MaxConcurrency),
	}
}

func (r *semaphoreHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	r.log.Debug("[Handler] Handle() ...", "msgIn", msg.ID())
	r.semaphore <- struct{}{}
	defer func() { <-r.semaphore }()
	msgOut, err := r.handler.Handle(msg)
	timespan := time.Since(begin).Seconds()
	if err != nil {
		r.log.Error("[Handler] Handle() err", "msgIn", msg.ID(),
			"err", err, "timespan", timespan)
		r.mxHandle.Observe(timespan, labelErr)
		return nil, err
	}
	r.log.Debug("[Handler] Handle() ok", "msgIn", msg.ID(),
		"msgOut", msgOut.ID(), "timespan", timespan)
	r.mxHandle.Observe(timespan, labelOk)
	return msgOut, nil
}

func (r *semaphoreHandler) GetMaxConcurrency() int {
	return cap(r.semaphore)
}

func (r *semaphoreHandler) GetCurrentConcurrency() int {
	return len(r.semaphore)
}

func (r *semaphoreHandler) GetNames() *Names {
	return &Names{
		Namespace:  r.namespace,
		Resilience: HandlerSemaphore,
		Name:       r.name,
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
				"gentle", HandlerCircuitBreaker,
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
func NewCircuitBreakerHandler(opts *CircuitBreakerHandlerOpts, handler Handler) Handler {
	return &circuitBreakerHandler{
		handlerFields: newHandlerFields(&opts.handlerOpts),
		mxCbErr:       opts.MetricCbErr,
		circuit:       opts.Circuit,
		handler:       handler,
	}
}

func (r *circuitBreakerHandler) Handle(msg Message) (Message, error) {
	begin := time.Now()
	r.log.Debug("[Handler] Handle() ...", "msgIn", msg.ID())
	result := make(chan interface{}, 1)
	err := hystrix.Do(r.circuit, func() error {
		msgOut, err := r.handler.Handle(msg)
		timespan := time.Since(begin).Seconds()
		if err != nil {
			r.log.Error("[Handler] Do()::Handle() err", "msgIn", msg.ID(),
				"err", err, "timespan", timespan)
			return err
		}
		r.log.Debug("[Handler] Do()::Handle() ok",
			"msgIn", msg.ID(), "msgOut", msgOut.ID(),
			"timespan", timespan)
		result <- msgOut
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
				"msgIn", msg.ID(), "err", err,
				"timespan", timespan)
			r.mxHandle.Observe(timespan, labelErr)
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
	msgOut := (<-result).(Message)
	timespan := time.Since(begin).Seconds()
	r.log.Debug("[Handler] Handle() ok", "msgIn", msg.ID(),
		"msgOut", msgOut.ID(), "timespan", timespan)
	r.mxHandle.Observe(timespan, labelOk)
	return msg, nil
}

func (r *circuitBreakerHandler) GetNames() *Names {
	return &Names{
		Namespace:  r.namespace,
		Resilience: HandlerCircuitBreaker,
		Name:       r.name,
	}
}

func (r *circuitBreakerHandler) GetCircuitName() string {
	return r.circuit
}

