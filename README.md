
##Go-gentle
Talk to external services like a gentleman.

This package provides resilient implementations of two interfaces: Stream and
Handler.

Stream has one method Get() that emits Message. Handler has one method Handle()
that transforms a given Message. The helper NewMappedStream() creates a
MappedStream whose Get() emits a Message transformed by a Handler from a given
Stream.

Moreover, resiliency patterns enable fault-tolerance as external services are
not always 100% reliable. Patterns in question here are rate limiting, retry,
circuit-breaker and bulkhead.

Each implementations of Stream and Handler features one resiliency pattern.
They are free to mix with each other to form a more sophisticated combined
resiliency.

```
func compose(name string, stream Stream, handler Handler) Stream {
	upstream := NewRetryStream(name,
		NewRateLimitedStream(name, stream,
			NewTokenBucketRateLimit(100, 1)),
		func() []time.Duration {
			return []time.Duration{time.Second, time.Second}
		})
	return NewMappedStream(name, upstream,
		NewCircuitBreakerHandler(name, handler, "circuit"))
}

```

##Document
[GoDoc](https://godoc.org/github.com/cfchou/go-gentle/service)

##Appendix

###state machine of circuits

```
Two rolling metrics are relevent:
reqs_last_10s := metrics.requests.take(-10s)
err_last_10s := metrics.errors.take(-10s) 
```

A circuit, according to its state, may allow or disallow requests.

Requests, if they are given the green light to run, would update the metrics.
If they are disallow, no update to metrics including __reqs_last_10s__.


```
[closed] -> [open] when:
reqs_last_10s > RequestVolumeThreshold &&
    err_last_10s / req_last_10s > ErrorPercentThreshold
```

```
[open] -> [half_closed] -> [closed] when:
now.Sub(openedOrLastTestTime) > SleepWindow && reqest.success
    
```
