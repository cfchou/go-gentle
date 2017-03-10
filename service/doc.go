/*
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
For example:
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


Author: Chifeng Chou
 */
package service

