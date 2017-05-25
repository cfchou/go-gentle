/*
This package provides composable resilient implementations of two interfaces:
Stream and Handler.

Stream and Handler and back-pressure

Stream and Handler are our fundamental abstractions to achieve back-pressure.
They are collectively called as mixins. Stream has one method Get() that emits
Message. Handler has another method Handle() that transforms a given Message. A
Stream may chain with other Streams and a Handler may chain with with other
Handlers. The helper NewHandlerStream() creates a HandlerStream whose Get()
emits a Message transformed by a Handler from a given Stream.

  Stream(https://godoc.org/github.com/cfchou/go-gentle/gentle#Stream)
  Handler(https://godoc.org/github.com/cfchou/go-gentle/gentle#Handler)
  NewHandlerStream()(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewHandlerStream)

Resiliency

Besides back-pressure, resiliency patterns are indispensable in distributed
systems as external services are not reliable at all time. Some of the patterns
come to useful include rate-limiting, retry(also known as back-off),
circuit-breaker and bulkhead. Each of our implementations of mixins features one
pattern:

  RateLimitedStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#RateLimitedStream)
  RetryStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#RetryStream)
  BulkheadStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#BulkheadStream)
  CircuitBreakerStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#CircuitBreakerStream)

  RateLimitedHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#RateLimitedHandler)
  RetryHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#RetryHandler)
  BulkheadHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#BulkheadHandler)
  CircuitBreakerHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#CircuitBreakerHandler)

Mixin creation and options

To create a mixin, either a Stream or a Handler, you must firstly create a
corresponding XXXOpts struct which can be done by using a helper. For instance,
NewRateLimitedStreamOpts() creates and initialises RateLimitedStreamOpts for
NewRateLimitedStream().

Helper NewXXXOpts() creates and initialises XXXOpts struct with facilities like
hierarchical naming, built-in logger but no support for metric collection.
XXXOpts is a plain-old structure you can overwrite fields to replace
logger or to add metric support, etc..

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
  	return NewHandlerStream(name, upstream,
  		NewCircuitBreakerHandler(name, userDefinedHandler, "circuit"))
  }

A crucial difference exists in how Stream and Handler apply resiliency patterns.
Take retry/back-off as an example, when a Stream observes a failure, it attempts
to pull again the NEXT Message from its upstream. On the other hand, when a
Handler sees a failure, it attempts to run again its wrapped-handler on the SAME
Message.

User defined Stream and Handler

Users can define their own Stream/Handler and compose them with our resilient
counterpart.

Meanwhile, a helper is provided for creating a Stream from a chan:

  NewChannelStream()(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewChannelStream)

Parallelism

We may want a Stream that fetches many Messages in parallel to achieve higher
throughput. That's when ConcurrentFetchStream comes into rescue. However, noted
that higher throughput is at the expense of breaking the order of Messages.

  ConcurrentFetchStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#ConcurrentFetchStream)



External References

Some of our implementations make heavy use of third-party packages. It helps to checkout their documents.

  Circuit-breaker is based on hystrix-go(https://godoc.org/github.com/afex/hystrix-go/hystrix).
  Rate-limiting is based on ratelimit(https://godoc.org/github.com/juju/ratelimit).
  Logging is based on log15(https://godoc.org/gopkg.in/inconshreveable/log15.v2).

*/
package gentle
