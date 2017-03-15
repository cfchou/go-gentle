
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
		[]time.Duration{time.Second, 2*time.Second, 4*time.Second})
	return NewMappedStream(name, upstream,
		NewCircuitBreakerHandler(name, userDefinedHandler, "circuit"))
}
```

## Document
[GoDoc](https://godoc.org/github.com/cfchou/go-gentle/gentle)

## Install

The master branch is considered unstable. Use [semantic versioning](http://gopkg.in/cfchou/go-gentle.v1/gentle) and verdor this library.

If you're using [glide](https://glide.sh/), simply run:
```
glide get gopkg.in/cfchou/go-gentle.v1
glide update
```

If you're not using package management tools, then
```
go get http://gopkg.in/cfchou/go-gentle.v1/gentle
```



