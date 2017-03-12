/*
This package provides composable resilient implementations of two interfaces:
Stream and Handler.

Stream and Handler and back-pressure

Stream and Handler are our fundamental abstractions to achieve back-pressure.
Stream has one method Get() that emits Message. Handler has another method
Handle() that transforms a given Message. The helper NewMappedStream()(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewMappedStream)
creates a MappedStream whose Get() emits a Message transformed by a Handler
from a given Stream.

Resiliency

Besides back-pressure, resiliency patterns are indispensable in distributed
systems as external services are not reliable at all time. Some of the patterns
come to useful include rate limiting, retry/back-off, circuit-breaker and bulkhead.
Each of our implementations of Stream and Handler features one resiliency
pattern:

RateLimitedStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#RateLimitedStream)
RetryStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#RetryStream)
BulkheadStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#BulkheadStream)
CircuitBreakerStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#CircuitBreakerStream)

RateLimitedHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#RateLimitedHandler)
RetryHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#RetryHandler)
BulkheadHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#BulkheadHandler)
CircuitBreakerHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#CircuitBreakerHandler)


Composability

Each of our implementations of Stream and Handler features one resiliency
pattern. Nevertheless, they and user-defined Stream/Handler are free to mix
with each other to form an ad-hoc, combined resiliency. For example:

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

User defined Stream and Handler

A helper is provided for creating a Stream from a chan.

NewChannelStream()(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewChannelStream)

Users define their own Stream/Handler and compose them with our resilient
counterpart.

Parallelism

We may want a Stream that fetches many Messages in parallel to achieve higher
throughput. That's when ConcurrentFetchStream comes into rescue. However, noted
that higher throughput is at the expense of breaking the order of Messages.

ConcurrentFetchStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#ConcurrentFetchStream)

*/
package gentle
