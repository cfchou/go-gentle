package gentle

import (
	"errors"
	"github.com/afex/hystrix-go/hystrix"
	log15 "gopkg.in/inconshreveable/log15.v2"
	prom "github.com/prometheus/client_golang/prometheus"
	"sync"
	"time"
)

var (
	// Histogram used by all Streams
	HistVec = prom.NewHistogramVec(
		prom.HistogramOpts {
			Namespace: "stream",
			Subsystem: "get",
			Name:"duration_seconds",
			Help:"Duration of Stream.Get() in seconds",
			Buckets: prom.DefBuckets,
		},
		[]string{"mixin", "name", "result"})
)

const (
	mixin_s_rate = "s_rate"
	mixin_s_retry = "s_retry"
	mixin_s_bulk = "s_bulk"
	mixin_s_cb = "s_circuit"
	mixin_s_ch = "s_chan"
	mixin_s_con = "s_con"
	mixin_s_map = "s_map"
)

// Rate limiting pattern is used to limit the speed of a series of Get().
type RateLimitedStream struct {
	Name    string
	Log     log15.Logger
	stream  Stream
	limiter RateLimit
	hist_ok prom.Histogram
	hist_err prom.Histogram
}

func NewRateLimitedStream(name string, stream Stream, limiter RateLimit) *RateLimitedStream {
	return &RateLimitedStream{
		Name:    name,
		Log:     Log.New("mixin", mixin_s_rate, "name", name),
		stream:  stream,
		limiter: limiter,
		hist_ok: HistVec.WithLabelValues(mixin_s_rate, name, "ok"),
		hist_err: HistVec.WithLabelValues(mixin_s_rate, name, "err"),
	}
}

// Get() is blocked when the limit is exceeded.
func (r *RateLimitedStream) Get() (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Stream] Get() ...")
	r.limiter.Wait(1, 0)
	msg, err := r.stream.Get()
	if err != nil {
		r.Log.Error("[Stream] Get() err", "err", err)
		r.hist_err.Observe(time.Now().Sub(begin).Seconds())
		return nil, err
	}
	r.Log.Debug("[Stream] Get() ok", "msg_out", msg.Id())
	r.hist_ok.Observe(time.Now().Sub(begin).Seconds())
	return msg, nil
}

// RetryStream will, when Get() encounters error, back off for some time
// and then retries.
type RetryStream struct {
	Name     string
	Log      log15.Logger
	stream   Stream
	backoffs []time.Duration
	hist_ok prom.Histogram
	hist_err prom.Histogram
}

func NewRetryStream(name string, stream Stream, backoffs []time.Duration) *RetryStream {
	if len(backoffs) == 0 {
		Log.Warn("NewRetryStream() len(backoffs) == 0")
	}
	return &RetryStream{
		Name:     name,
		Log:      Log.New("mixin", mixin_s_retry, "name", name),
		stream:   stream,
		backoffs: backoffs,
		hist_ok: HistVec.WithLabelValues(mixin_s_retry, name, "ok"),
		hist_err: HistVec.WithLabelValues(mixin_s_retry, name, "err"),
	}
}

func (r *RetryStream) Get() (Message, error) {
	begin := time.Now()
	bk := r.backoffs
	to_wait := 0 * time.Second
	for {
		r.Log.Debug("[Stream] Get() ...", "count",
			len(r.backoffs)-len(bk)+1, "wait", to_wait)
		// A negative or zero duration causes Sleep to return immediately.
		time.Sleep(to_wait)
		// assert end_allowed.Sub(now) != 0
		msg, err := r.stream.Get()
		if err == nil {
			timespan := time.Now().Sub(begin).Seconds()
			r.Log.Debug("[Stream] Get() ok", "msg_out", msg.Id(),
				"timespan", timespan)
			r.hist_ok.Observe(timespan)
			return msg, nil
		}
		if len(bk) == 0 {
			timespan := time.Now().Sub(begin).Seconds()
			r.Log.Error("[Streamer] Get() err and no more backing off",
				"err", err, "timespan", timespan)
			r.hist_err.Observe(timespan)
			return nil, err
		} else {
			timespan := time.Now().Sub(begin)
			r.Log.Error("[Stream] Get() err, backing off ...",
				"err", err, "timespan", timespan)
			to_wait = bk[0]
			bk = bk[1:]
		}
	}
}

// Bulkhead pattern is used to limit the number of concurrent Get().
type BulkheadStream struct {
	Name      string
	Log       log15.Logger
	stream    Stream
	semaphore chan *struct{}
	hist_ok prom.Histogram
	hist_err prom.Histogram
}

// Create a BulkheadStream that allows at maximum $max_concurrency Get() to
// run concurrently.
func NewBulkheadStream(name string, stream Stream, max_concurrency int) *BulkheadStream {
	if max_concurrency <= 0 {
		panic(errors.New("max_concurrent must be greater than 0"))
	}
	return &BulkheadStream{
		Name:      name,
		Log:       Log.New("mixin", mixin_s_bulk, "name", name),
		stream:    stream,
		semaphore: make(chan *struct{}, max_concurrency),
		hist_ok: HistVec.WithLabelValues(mixin_s_bulk, name, "ok"),
		hist_err: HistVec.WithLabelValues(mixin_s_bulk, name, "err"),
	}
}

// Get() is blocked when the limit is exceeded.
func (r *BulkheadStream) Get() (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Stream] Get() ...")
	r.semaphore <- &struct{}{}
	msg, err := r.stream.Get()
	if err != nil {
		r.Log.Error("[Stream] Get() err", "err", err)
		<-r.semaphore
		r.hist_err.Observe(time.Now().Sub(begin).Seconds())
		return nil, err
	}
	r.Log.Debug("[Stream] Get() ok", "msg_out", msg.Id())
	<-r.semaphore
	r.hist_ok.Observe(time.Now().Sub(begin).Seconds())
	return msg, nil
}

// CircuitBreakerStream is a Stream equipped with a circuit-breaker.
type CircuitBreakerStream struct {
	Name    string
	Log     log15.Logger
	Circuit string
	stream  Stream
	hist_ok prom.Histogram
	hist_err prom.Histogram
}

// In hystrix-go, a circuit-breaker must be given a unique name.
// NewCircuitBreakerStream() creates a CircuitBreakerStream with a
// circuit-breaker named $circuit.
func NewCircuitBreakerStream(name string, stream Stream, circuit string) *CircuitBreakerStream {
	return &CircuitBreakerStream{
		Name: name,
		Log: Log.New("mixin", mixin_s_cb, "name", name,
			"circuit", circuit),
		Circuit: circuit,
		stream:  stream,
		hist_ok: HistVec.WithLabelValues(mixin_s_cb, name, "ok"),
		hist_err: HistVec.WithLabelValues(mixin_s_cb, name, "err"),
	}
}

func (r *CircuitBreakerStream) Get() (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Stream] Get() ...")
	result := make(chan *tuple, 1)
	err := hystrix.Do(r.Circuit, func() error {
		msg, err := r.stream.Get()
		if err != nil {
			r.Log.Error("[Stream] Get() err", "err", err)
			result <- &tuple{
				fst: msg,
				snd: err,
			}
			return err
		}
		r.Log.Debug("[Stream] Get() ok", "msg_out", msg.Id())
		result <- &tuple{
			fst: msg,
			snd: err,
		}
		return nil
	}, nil)
	// hystrix.ErrTimeout doesn't interrupt work anyway.
	// It just contributes to circuit's metrics.
	if err != nil {
		r.Log.Warn("[Stream] Circuit err", "err", err)
		if err != hystrix.ErrTimeout {
			// Can be ErrCircuitOpen, ErrMaxConcurrency or
			// Get()'s err.
			r.hist_err.Observe(time.Now().Sub(begin).Seconds())
			return nil, err
		}
	}
	tp := <-result
	if tp.snd == nil {
		return tp.fst.(Message), nil
	}
	r.hist_ok.Observe(time.Now().Sub(begin).Seconds())
	return nil, tp.snd.(error)
}

// ChannelStream forms a stream from a channel.
type ChannelStream struct {
	Name    string
	Log     log15.Logger
	channel <-chan Message
	hist_ok prom.Histogram
}

// Create a ChannelStream that gets Messages from $channel.
func NewChannelStream(name string, channel <-chan Message) *ChannelStream {
	return &ChannelStream{
		Name:    name,
		Log:     Log.New("mixin", mixin_s_ch, "name", name),
		channel: channel,
		hist_ok: HistVec.WithLabelValues(mixin_s_ch, name, "ok"),
	}
}

func (r *ChannelStream) Get() (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Stream] Get() ...")
	msg := <-r.channel
	r.Log.Debug("[Stream] Get() ok", "msg_out", msg.Id())
	r.hist_ok.Observe(time.Now().Sub(begin).Seconds())
	return msg, nil
}

// ConcurrentFetchStream internally keeps fetching a number of items from
// upstream concurrently.
// Note that the order of messages emitted from the upstream may not be
// preserved. It's down to application to maintain the order if that's required.
type ConcurrentFetchStream struct {
	Name      string
	Log       log15.Logger
	stream    Stream
	receives  chan *tuple
	semaphore chan *struct{}
	once      sync.Once
	hist_ok prom.Histogram
	hist_err prom.Histogram
}

// Create a ConcurrentFetchStream that allows at maximum $max_concurrency
// Messages being internally fetched from upstream concurrently.
func NewConcurrentFetchStream(name string, stream Stream, max_concurrency int) *ConcurrentFetchStream {
	return &ConcurrentFetchStream{
		Name:      name,
		Log:       Log.New("mixin", mixin_s_con, "name", name),
		stream:    stream,
		receives:  make(chan *tuple, max_concurrency),
		semaphore: make(chan *struct{}, max_concurrency),
		hist_ok: HistVec.WithLabelValues(mixin_s_con, name, "ok"),
		hist_err: HistVec.WithLabelValues(mixin_s_con, name, "err"),
	}
}

func (r *ConcurrentFetchStream) onceDo() {
	go func() {
		r.Log.Info("[Stream] once")
		for {
			// pull more messages as long as semaphore allows
			r.semaphore <- &struct{}{}
			// Since Get() are run concurrently, the order of
			// elements from upstream may not preserved.
			go func() {
				r.Log.Debug("[Stream] onceDo Get() ...")
				msg, err := r.stream.Get()
				if err == nil {
					r.Log.Debug("[Stream] onceDo Get() ok",
						"msg_out", msg.Id())
				} else {
					r.Log.Error("[Stream] onceDo Get() err",
						"err", err)
				}
				r.receives <- &tuple{
					fst: msg,
					snd: err,
				}
			}()
		}
	}()
}

func (r *ConcurrentFetchStream) Get() (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Stream] Get() ...")
	r.once.Do(r.onceDo)
	tp := <-r.receives
	<-r.semaphore
	if tp.snd != nil {
		err := tp.snd.(error)
		r.Log.Error("[Stream] Get() err", "err", err)
		r.hist_err.Observe(time.Now().Sub(begin).Seconds())
		return nil, err
	}
	msg := tp.fst.(Message)
	r.Log.Debug("[Stream] Get() ok", "msg_out", msg.Id())
	r.hist_ok.Observe(time.Now().Sub(begin).Seconds())
	return msg, nil
}

// A MappedStream whose Get() emits a Message transformed by a Handler from
// a given Stream.
type MappedStream struct {
	Name    string
	Log     log15.Logger
	stream  Stream
	handler Handler
	hist_ok prom.Histogram
	hist_err prom.Histogram
}

func NewMappedStream(name string, stream Stream, handler Handler) *MappedStream {
	return &MappedStream{
		Name:    name,
		Log:     Log.New("mixin", mixin_s_map, "name", name),
		stream:  stream,
		handler: handler,
		hist_ok: HistVec.WithLabelValues(mixin_s_map, name, "ok"),
		hist_err: HistVec.WithLabelValues(mixin_s_map, name, "err"),
	}
}

func (r *MappedStream) Get() (Message, error) {
	begin := time.Now()
	r.Log.Debug("[Stream] Get() ...")
	msg, err := r.stream.Get()
	if err != nil {
		r.Log.Error("[Stream] Get() err", "err", err)
		r.hist_err.Observe(time.Now().Sub(begin).Seconds())
		return nil, err
	}
	r.Log.Debug("[Stream] Get() ok, Handle() ...", "msg", msg.Id())
	hmsg, herr := r.handler.Handle(msg)
	if herr != nil {
		r.Log.Error("[Stream] Handle() err", "err", herr)
		r.hist_err.Observe(time.Now().Sub(begin).Seconds())
		return nil, herr
	}
	r.Log.Debug("[Stream] Handle() ok", "msg_in", msg.Id(),
		"msg_out", hmsg.Id())
	r.hist_ok.Observe(time.Now().Sub(begin).Seconds())
	return hmsg, nil
}
