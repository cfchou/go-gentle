/*
Package gentle provides composable resilient implementations of two interfaces:
Stream and Handler.

Stream and Handler

Stream and Handler are our fundamental abstractions to achieve back-pressure.
Stream has Get() that returns Message. Handler has Handle() that transforms a
given Message.

  Stream(https://godoc.org/github.com/cfchou/go-gentle/gentle#Stream)
  Handler(https://godoc.org/github.com/cfchou/go-gentle/gentle#Handler)

Developers should implement their own Stream/Handler logic. These two named
types help to directly use a function as a Stream/Handler.

  SimpleStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#SimpleStream)
  SimpleHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#SimpleHandler)

A Stream/Handler can chain with an arbitrary number of Handlers. Their semantic
is that any failing element in the chain would skip the rest of all. Also note
that any element can also be a nested chain itself.

  AppendHandlersStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendHandlersStream)
  AppendHandlersHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendHandlersHandler)

If simply appending Streams/Handlers is not enough, like these resilience
Streams/Handlers defined in this package, developers can form a Stream/Handler
with an advanced flow control by embedding other Streams/Handlers.

Our Resilience Streams and Handlers

Besides back-pressure, resiliency patterns are indispensable in distributed
systems as external services are not reliable at all time. Some of the patterns
come to useful include rate-limiting, retry(also known as back-off),
circuit-breaker and bulkhead. Each of our implementations of resilience features one
pattern:

  rateLimitedStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRateLimitedStream)
  retryStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRetryStream)
  bulkheadStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewBulkheadStream)
  circuitBreakerStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewCircuitBreakerStream)

  rateLimitedHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRateLimitedHandler)
  retryHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRetryHandler)
  bulkheadHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewBulkheadHandler)
  circuitBreakerHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewCircuitBreakerHandler)

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
  	return NewHandlerMappedStream(name, upstream,
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

Note

The implementation of Stream.Get() and Handler.Handle() should be thread-safe.
A good practice is to make Stream/Handler state-less. A Message needs not to be
immutable but it's good to be so. Our resilience Streams/Handlers are all
thread-safe and don't mutate Messages.

External References

Some of our implementations make heavy use of third-party packages. It helps to checkout their documents.

  Circuit-breaker is based on hystrix-go(https://godoc.org/github.com/afex/hystrix-go/hystrix).
  Rate-limiting is based on ratelimit(https://godoc.org/github.com/juju/ratelimit).
  Logging is based on log15(https://godoc.org/gopkg.in/inconshreveable/log15.v2).

*/
package gentle
