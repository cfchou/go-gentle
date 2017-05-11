package gentle

import (
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	"time"
	"github.com/benbjohnson/clock"
)

const (
	// Stream types(mixins), are most often used as part of RegistryKey.
	MIXIN_STREAM_RATELIMITED     = "sRate"
	MIXIN_STREAM_RETRY           = "sRetry"
	MIXIN_STREAM_BULKHEAD        = "sBulk"
	MIXIN_STREAM_CIRCUITBREAKER  = "sCircuit"
	MIXIN_STREAM_CHANNEL         = "sChan"
	MIXIN_STREAM_HANDLED         = "sHan"
	MIXIN_STREAM_TRANS           = "sTrans"
)

var (
	label_ok  = map[string]string{"result": "ok"}
	label_err = map[string]string{"result": "err"}
)

type StreamOpts struct {
	Namespace      string
	Name           string
	Log            Logger
	Clock 	       clock.Clock
	MetricGet 	       Metric
}

type RateLimitedStreamOpts struct {
	StreamOpts
	Limiter        RateLimit
}

func NewRateLimitedStreamOpts(namespace, name string, limiter RateLimit) *RateLimitedStreamOpts {
	return &RateLimitedStreamOpts{
		StreamOpts: StreamOpts{
			Namespace: namespace,
			Name:name,
			Log: Log.New("namespace", namespace,
				"mixin", MIXIN_STREAM_RATELIMITED, "name", name),
			Clock: clock.New(),
			MetricGet: noopMetric,
		},
		Limiter: limiter,
	}
}

// Rate limiting pattern is used to limit the speed of a series of Get().
type RateLimitedStream struct {
	namespace      string
	name           string
	log            Logger
	clock 	       clock.Clock
	mxGet 	       Metric
	limiter        RateLimit
	stream         Stream
}

func NewRateLimitedStream(opts RateLimitedStreamOpts, upstream Stream) *RateLimitedStream {
	return &RateLimitedStream{
		namespace: opts.Namespace,
		name:      opts.Name,
		log:       opts.Log,
		clock:     opts.Clock,
		mxGet:     opts.MetricGet,
		limiter:   opts.Limiter,
		stream:    upstream,
	}
}

// Get() is blocked when the limit is exceeded.
func (r *RateLimitedStream) Get() (Message, error) {
	begin := r.clock.Now()
	r.log.Debug("[Stream] Get() ...")
	r.limiter.Wait(1, 0)
	msg, err := r.stream.Get()
	timespan := r.clock.Now().Sub(begin).Seconds()
	if err != nil {
		r.log.Error("[Stream] Get() err", "err", err,
			"timespan", timespan)
		r.mxGet.Observe(timespan, label_err)
		return nil, err
	}
	r.log.Debug("[Stream] Get() ok", "msg_out", msg.Id(),
		"timespan", timespan)
	r.mxGet.Observe(timespan, label_ok)
	return msg, nil
}

func (r *RateLimitedStream) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin: MIXIN_STREAM_RATELIMITED,
		Name: r.name,
	}
}

type RetryStreamOpts struct {
	StreamOpts
	MetricTryNum       Metric
	BackOff    BackOff
}

func NewRetryStreamOpts(namespace, name string, backoff BackOff) *RetryStreamOpts {
	return &RetryStreamOpts{
		StreamOpts: StreamOpts{
			Namespace: namespace,
			Name:name,
			Log: Log.New("namespace", namespace, "mixin",
				MIXIN_STREAM_RETRY, "name", name),
			Clock: clock.New(),
			MetricGet: noopMetric,
		},
		MetricTryNum: noopMetric,
		BackOff: backoff,
	}
}

// RetryStream will, when Get() encounters error, back off for some time
// and then retries.
type RetryStream struct {
	namespace string
	name      string
	log       Logger
	clock     clock.Clock
	mxGet     Metric
	obTryNum  Metric
	backoff   BackOff
	stream    Stream
}

func NewRetryStream(opts RetryStreamOpts, upstream Stream) *RetryStream {
	return &RetryStream{
		namespace: opts.Namespace,
		name:      opts.Name,
		log:       opts.Log,
		clock:     opts.Clock,
		mxGet:     opts.MetricGet,
		obTryNum:  opts.MetricTryNum,
		backoff:   opts.BackOff,
		stream:    upstream,
	}
}

func (r *RetryStream) Get() (Message, error) {
	begin := r.clock.Now()
	count := 1
	r.log.Debug("[Stream] Get() ...", "count", count)
	for {
		msg, err := r.stream.Get()
		if err == nil {
			timespan := r.clock.Now().Sub(begin).Seconds()
			r.log.Debug("[Stream] Get() ok", "msg_out", msg.Id(),
				"timespan", timespan, "count", count)
			r.mxGet.Observe(timespan, label_ok)
			r.obTryNum.Observe(float64(count), label_ok)
			return msg, nil
		}
		to_wait := r.backoff.Next()
		// Next() should immediately return but we can't guarantee so
		// timespan is calculated after Next().
		timespan := r.clock.Now().Sub(begin).Seconds()
		if to_wait == BackOffStop {
			r.log.Error("[Streamer] Get() err and no more backing off",
				"err", err, "timespan", timespan,
				"count", count)
			r.mxGet.Observe(timespan, label_err)
			r.obTryNum.Observe(float64(count), label_err)
			return nil, err
		}
		// timespan in our convention is used to track the overall
		// time of current function. Here we record time
		// passed as "elapse".
		count++
		r.log.Error("[Stream] Get() err, backing off ...",
			"err", err, "elapse", timespan, "count", count,
			"wait", to_wait)
		r.clock.Sleep(to_wait)
	}
}

func (r *RetryStream) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin: MIXIN_STREAM_RETRY,
		Name: r.name,
	}
}

type BulkheadStreamOpts struct {
	StreamOpts
	MaxConcurrency int
}

func NewBulkheadStreamOpts(namespace, name string, max_concurrency int) *BulkheadStreamOpts {
	if max_concurrency <= 0 {
		panic(errors.New("max_concurrent must be greater than 0"))
	}

	return &BulkheadStreamOpts{
		StreamOpts: StreamOpts{
			Namespace: namespace,
			Name:name,
			Log: Log.New("namespace", namespace, "mixin",
				MIXIN_STREAM_BULKHEAD, "name", name),
			Clock: clock.New(),
			MetricGet: noopMetric,
		},
		MaxConcurrency: max_concurrency,
	}
}

// Bulkhead pattern is used to limit the number of concurrently hanging Get().
// It uses semaphore isolation, similar to the approach used in hystrix.
// http://stackoverflow.com/questions/30391809/what-is-bulkhead-pattern-used-by-hystrix
type BulkheadStream struct {
	namespace      string
	name           string
	log            Logger
	clock 	       clock.Clock
	stream         Stream
	mxGet 	       Metric
	semaphore      chan struct{}
}

// Create a BulkheadStream that allows at maximum $max_concurrency Get() to
// run concurrently.
func NewBulkheadStream(opts BulkheadStreamOpts, upstream Stream) *BulkheadStream {

	return &BulkheadStream{
		namespace: opts.Namespace,
		name:      opts.Name,
		log:       opts.Log,
		clock:     opts.Clock,
		mxGet:     opts.MetricGet,
		stream:    upstream,
		semaphore: make(chan struct{}, opts.MaxConcurrency),
	}
}

// Get() is blocked when the limit is exceeded.
func (r *BulkheadStream) Get() (Message, error) {
	begin := r.clock.Now()
	r.log.Debug("[Stream] Get() ...")
	select {
	case r.semaphore <- struct{}{}:
		defer func() {
			<-r.semaphore
		}()
		msg, err := r.stream.Get()
		timespan := r.clock.Now().Sub(begin).Seconds()
		if err != nil {
			r.log.Error("[Stream] Get() err", "err", err,
				"timespan", timespan)
			r.mxGet.Observe(timespan, label_err)
			return nil, err
		}
		r.log.Debug("[Stream] Get() ok", "msg_out", msg.Id(),
			"timespan", timespan)
		r.mxGet.Observe(timespan, label_ok)
		return msg, nil
	default:
		r.log.Error("[Stream] Get() err", "err", ErrMaxConcurrency)
		return nil, ErrMaxConcurrency
	}
}

func (r *BulkheadStream) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin: MIXIN_STREAM_BULKHEAD,
		Name: r.name,
	}
}

const (
	// CbTimeoutDefault is how long to wait for command to complete, in
	// milliseconds
	CbTimeoutDefault = 10 * time.Second

	// CbMaxConcurrentDefault is how many commands of the same type can run
	// at the same time
	CbMaxConcurrentDefault = 1024

	// CbVolumeThresholdDefault is the minimum number of requests needed
	// before a circuit can be tripped due to health
	CbVolumeThresholdDefault = 20

	// CbErrorPercentThresholdDefault causes circuits to open once the
	// rolling measure of errors exceeds this percent of requests
	CbErrorPercentThresholdDefault = 50

	// CbSleepWindowDefault is how long, in milliseconds, to wait after a
	// circuit opens before testing for recovery
	CbSleepWindowDefault = 5 * time.Second
)

type CircuitBreakerConf struct {
	CbTimeout               time.Duration
	CbMaxConcurrent         int
	CbVolumeThreshold       uint64
	CbErrorPercentThreshold int
	CbSleepWindow           time.Duration
}

var cbConfdefault = CircuitBreakerConf{
	CbTimeout:               CbTimeoutDefault,
	CbMaxConcurrent:         CbMaxConcurrentDefault,
	CbVolumeThreshold:       CbVolumeThresholdDefault,
	CbErrorPercentThreshold: CbErrorPercentThresholdDefault,
	CbSleepWindow:           CbSleepWindowDefault,
}

func NewDefaultCircuitBreakerConf() *CircuitBreakerConf {
	return &CircuitBreakerConf{
		CbTimeout:               CbTimeoutDefault,
		CbMaxConcurrent:         CbMaxConcurrentDefault,
		CbVolumeThreshold:       CbVolumeThresholdDefault,
		CbErrorPercentThreshold: CbErrorPercentThresholdDefault,
		CbSleepWindow:           CbSleepWindowDefault,
	}
}

func (c *CircuitBreakerConf) RegisterAs(circuit string) {
	hystrix.ConfigureCommand(circuit, hystrix.CommandConfig{
		Timeout:                int(c.CbTimeout / time.Millisecond),
		MaxConcurrentRequests:  c.CbMaxConcurrent,
		RequestVolumeThreshold: c.CbErrorPercentThreshold,
		SleepWindow:            int(c.CbSleepWindow / time.Millisecond),
		ErrorPercentThreshold:  c.CbErrorPercentThreshold,
	})
}

type CircuitBreakerStreamOpts struct {
	StreamOpts
	MetricCbErr     Metric
	Circuit   string
}

func NewCircuitBreakerStreamOpts(namespace, name, circuit string) *CircuitBreakerStreamOpts {
	return &CircuitBreakerStreamOpts{
		StreamOpts: StreamOpts{
			Namespace: namespace,
			Name:name,
			Log: Log.New("namespace", namespace,
				"mixin", MIXIN_STREAM_CIRCUITBREAKER,
				"name", name, "circuit", circuit),
			Clock: clock.New(),
			MetricGet: noopMetric,
		},
		MetricCbErr: noopMetric,
		Circuit: circuit,
	}
}

// CircuitBreakerStream is a Stream equipped with a circuit-breaker.
type CircuitBreakerStream struct {
	namespace      string
	name           string
	log            Logger
	clock 	       clock.Clock
	mxGet Metric
	mxCbErr Metric
	circuit        string
	stream         Stream
}

func (r *CircuitBreakerStream) GetCircuitName() string {
	return r.circuit
}

// In hystrix-go, a circuit-breaker must be given a unique name.
// NewCircuitBreakerStream() creates a CircuitBreakerStream with a
// circuit-breaker named $circuit.
func NewCircuitBreakerStream(opts CircuitBreakerStreamOpts, stream Stream) *CircuitBreakerStream {

	// Note that if it might overwrite or be overwritten by concurrently
	// registering the same circuit.
	allCircuits := hystrix.GetCircuitSettings()
	if _, ok := allCircuits[opts.Circuit]; !ok {
		NewDefaultCircuitBreakerConf().RegisterAs(opts.Circuit)
	}

	return &CircuitBreakerStream{
		namespace: opts.Namespace,
		name:      opts.Name,
		log:  opts.Log,
		clock:   opts.Clock,
		mxGet:   opts.MetricGet,
		mxCbErr:   opts.MetricCbErr,
		circuit: opts.Circuit,
		stream:  stream,
	}
}

func (r *CircuitBreakerStream) Get() (Message, error) {
	begin := r.clock.Now()
	r.log.Debug("[Stream] Get() ...")
	result := make(chan Message, 1)
	err := hystrix.Do(r.circuit, func() error {
		msg, err := r.stream.Get()
		timespan := r.clock.Now().Sub(begin).Seconds()
		if err != nil {
			r.log.Error("[Stream] Get() in CB err",
				"err", err, "timespan", timespan)
			return err
		}
		r.log.Debug("[Stream] Get() in CB ok",
			"msg_out", msg.Id(), "timespan", timespan)
		result <- msg
		return nil
	}, nil)
	if err != nil {
		defer func() {
			timespan := r.clock.Now().Sub(begin).Seconds()
			r.log.Error("[Stream] Circuit err", "err", err,
				"timespan", timespan)
			r.mxGet.Observe(timespan, label_err)
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
	msg := <-result
	timespan := r.clock.Now().Sub(begin).Seconds()
	r.log.Debug("[Stream] Get() ok", "msg_out", msg.Id(),
		"timespan", timespan)
	r.mxGet.Observe(timespan, label_ok)
	return msg, nil
}

func (r *CircuitBreakerStream) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin: MIXIN_STREAM_CIRCUITBREAKER,
		Name: r.name,
	}
}

type ChannelStreamOpts struct {
	StreamOpts
	Channel        <-chan interface{}
}

func NewChannelStreamOpts(namespace, name string, channel <-chan interface{}) *ChannelStreamOpts {
	return &ChannelStreamOpts{
		StreamOpts: StreamOpts{
			Namespace: namespace,
			Name:      name,
			Log:       Log.New("namespace", namespace, "mixin",
				MIXIN_STREAM_CHANNEL, "name", name),
			Clock:     clock.New(),
			MetricGet:     noopMetric,
		},
		Channel: channel,
	}
}

// ChannelStream forms a stream from a channel.
type ChannelStream struct {
	namespace      string
	name           string
	log            Logger
	clock 	       clock.Clock
	mxGet 	       Metric
	channel        <-chan interface{}
}

// Create a ChannelStream that gets Messages from $channel.
func NewChannelStream(opts ChannelStreamOpts) *ChannelStream {
	return &ChannelStream{
		namespace: opts.Namespace,
		name:      opts.Name,
		log:       opts.Log,
		mxGet:     opts.MetricGet,
		channel:   opts.Channel,
	}
}

func (r *ChannelStream) Get() (Message, error) {
	begin := r.clock.Now()
	r.log.Debug("[Stream] Get() ...")
	switch v := (<-r.channel).(type) {
	case Message:
		timespan := r.clock.Now().Sub(begin).Seconds()
		r.log.Debug("[Stream] Get() ok", "msg_out", v.Id(),
			"timespan", timespan)
		r.mxGet.Observe(timespan, label_ok)
		return v, nil
	case error:
		timespan := r.clock.Now().Sub(begin).Seconds()
		r.log.Debug("[Stream] Get() err", "err", v,
			"timespan", timespan)
		r.mxGet.Observe(timespan, label_err)
		return nil, v
	default:
		timespan := r.clock.Now().Sub(begin).Seconds()
		r.log.Error("[Stream] Get() err, invalid type",
			"value", v, "timespan", timespan)
		return nil, ErrInvalidType
	}
}

func (r *ChannelStream) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin: MIXIN_STREAM_CHANNEL,
		Name: r.name,
	}
}

type HandlerStreamOpts StreamOpts

func NewHandlerStreamOpts(namespace, name string) *HandlerStreamOpts {
	return &HandlerStreamOpts{
		Namespace: namespace,
		Name:      name,
		Log:       Log.New("namespace", namespace, "mixin",
			MIXIN_STREAM_HANDLED, "name", name),
		Clock:     clock.New(),
		MetricGet:     noopMetric,
	}
}

// A HandlerStream whose Get() emits a Message transformed by a Handler from
// a given Stream.
type HandlerStream struct {
	namespace      string
	name           string
	log            Logger
	clock clock.Clock
	stream         Stream
	handler        Handler
	mxGet Metric
}

func NewHandlerStream(opts HandlerStreamOpts, upstream Stream, handler Handler) *HandlerStream {
	return &HandlerStream{
		namespace: opts.Namespace,
		name:      opts.Name,
		log:       opts.Log,
		clock: opts.Clock,
		mxGet: opts.MetricGet,
		stream:    upstream,
		handler:   handler,
	}
}

func (r *HandlerStream) Get() (Message, error) {
	begin := r.clock.Now()
	r.log.Debug("[Stream] Get() ...")
	msg, err := r.stream.Get()
	if err != nil {
		r.log.Error("[Stream] Get() err", "err", err)
		r.mxGet.Observe(r.clock.Now().Sub(begin).Seconds(), label_err)
		return nil, err
	}
	r.log.Debug("[Stream] Get() ok, Handle() ...", "msg", msg.Id())
	hmsg, herr := r.handler.Handle(msg)
	timespan := r.clock.Now().Sub(begin).Seconds()
	if herr != nil {
		r.log.Error("[Stream] Handle() err", "err", herr,
			"timespan", timespan)
		r.mxGet.Observe(timespan, label_err)
		return nil, herr
	}
	r.log.Debug("[Stream] Handle() ok", "msg_in", msg.Id(),
		"msg_out", hmsg.Id(), "timespan", timespan)
	r.mxGet.Observe(timespan, label_ok)
	return hmsg, nil
}

func (r *HandlerStream) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin: MIXIN_STREAM_HANDLED,
		Name: r.name,
	}
}

type TransformStreamOpts struct {
	StreamOpts
	TransFunc      func(Message, error) (Message, error)
}

func NewTransformStreamOpts(namespace, name string,
	transFunc func(Message, error) (Message, error)) *TransformStreamOpts {
	return &TransformStreamOpts{
		StreamOpts: StreamOpts{
			Namespace: namespace,
			Name:name,
			Log: Log.New("namespace", namespace,
				"mixin", MIXIN_STREAM_TRANS, "name", name),
			Clock: clock.New(),
			MetricGet: noopMetric,
		},
		TransFunc: transFunc,
	}
}

// TransformStream transforms what Stream.Get() returns.
type TransformStream struct {
	namespace string
	name      string
	log       Logger
	clock     clock.Clock
	mxGet     Metric
	stream    Stream
	transFunc func(Message, error) (Message, error)
}

func NewTransformStream(opts TransformStreamOpts, upstream Stream) *TransformStream {
	return &TransformStream{
		namespace: opts.Namespace,
		name:      opts.Name,
		log:       opts.Log,
		clock: opts.Clock,
		mxGet: opts.MetricGet,
		stream:    upstream,
		transFunc: opts.TransFunc,
	}
}

func (r *TransformStream) Get() (Message, error) {
	begin := r.clock.Now()
	r.log.Debug("[Stream] Get() ...")
	msg_mid, err := r.stream.Get()
	if err != nil {
		r.log.Debug("[Stream] Get() err, transFunc() ...",
			"err", err)
		// enforce the exclusivity
		msg_mid = nil
	} else {
		r.log.Debug("[Stream] Get() ok, transFunc() ...",
			"msg_mid", msg_mid.Id())
	}
	msg_out, err2 := r.transFunc(msg_mid, err)
	timespan := r.clock.Now().Sub(begin).Seconds()
	if err2 != nil {
		if msg_mid != nil {
			r.log.Error("[Stream] transFunc() err",
				"msg_mid", msg_mid.Id(), "err", err2,
				"timespan", timespan)
		} else {
			r.log.Error("[Stream] transFunc() err",
				"err", err2, "timespan", timespan)
		}
		r.mxGet.Observe(timespan, label_err)
		return nil, err2
	}
	if msg_mid != nil {
		r.log.Debug("[Stream] transFunc() ok",
			"msg_mid", msg_mid.Id(),
			"msg_out", msg_out.Id(), "timespan", timespan)
	} else {
		r.log.Debug("[Stream] transFunc() ok",
			"msg_out", msg_out.Id(), "timespan", timespan)

	}
	r.mxGet.Observe(timespan, label_ok)
	return msg_out, nil
}

func (r *TransformStream) GetNames() *Names {
	return &Names{
		Namespace: r.namespace,
		Mixin: MIXIN_STREAM_TRANS,
		Name: r.name,
	}
}

