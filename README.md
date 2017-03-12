
## Go-gentle
Talk to external services like a gentleman.

## Intro
This package provides composable resilient implementations of two interfaces:
Stream and Handler.


## Stream, Handler and back-pressure
__Stream__ and __Handler__ are our fundamental abstractions to achieve back-pressure.
Stream has one method __Get()__ that emits Message. Handler has another method
__Handle()__ that transforms a given Message. The helper [NewMappedStream()](https://godoc.org/github.com/cfchou/go-gentle/gentle#NewMappedStream)
creates a MappedStream whose Get() emits a Message transformed by a Handler
from a given Stream.

## Resiliency
Besides back-pressure, resiliency patterns are indispensable in distributed
systems as external services are not reliable at all time. Some of the patterns
come to useful include __rate limiting, retry/back-off, circuit-breaker and bulkhead__.
Each of our implementations of Stream and Handler features one resiliency
pattern.

## Composability
_Users define their own Stream/Handler and compose them with our resilient
counterpart_.

Each of our implementations of Stream and Handler features one resiliency
pattern. Nevertheless, _they are free to mix with each other to form an ad-hoc,
combined resiliency_. For example:
```
func compose(name string, userDefinedStream Stream, userDefinedHandler Handler) Stream {
	upstream := NewRetryStream(name,
		NewRateLimitedStream(name, userDefinedStream,
			NewTokenBucketRateLimit(100, 1)),
		func() []time.Duration {
			return []time.Duration{time.Second, time.Second}
		})
	return NewMappedStream(name, upstream,
		NewCircuitBreakerHandler(name, userDefinedHandler, "circuit"))
}
```

## Install

## Document
[GoDoc](https://godoc.org/github.com/cfchou/go-gentle/gentle)

## Appendix

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
