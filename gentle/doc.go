/*
Package gentle defines Stream and Handler interfaces and provides composable
resilient implementations of them.


Stream and Handler

Stream and Handler are our fundamental abstractions to achieve back-pressure.
Stream has Get() that emits Messages. Handler has Handle() that transforms
given Messages.

  Stream(https://godoc.org/github.com/cfchou/go-gentle/gentle#Stream)
  Handler(https://godoc.org/github.com/cfchou/go-gentle/gentle#Handler)

Developers should implement their own logic in the forms of Stream/Handler.
For simple cases, these named types SimpleStream and SimpleHandler help to
directly use a function as a Stream/Handler.

  SimpleStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#SimpleStream)
  SimpleHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#SimpleHandler)

Our Resilience Streams and Handlers

Resiliency patterns are indispensable in distributed systems because external
services are not reliable at all time. We provide some useful patterns in the
forms of Streams/Handlers. They include rate-limiting, retry(also known as back-off),
bulkhead and circuit-breaker. Each of them can be freely composed with other
Streams/Handlers as one sees fit.

  rateLimitedStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRateLimitedStream)
  retryStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRetryStream)
  bulkheadStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewBulkheadStream)
  circuitStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewCircuitStream)

  rateLimitedHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRateLimitedHandler)
  retryHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRetryHandler)
  bulkheadHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewBulkheadHandler)
  circuitHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewCircuitHandler)

Composability

A Stream/Handler can chain with an arbitrary number of Handlers. Their semantic
is that any failing element in the chain would skip the rest of all. Also note
that any element can also be a nested chain itself.

  AppendHandlersStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendHandlersStream)
  AppendHandlersHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendHandlersHandler)

If simply appending Streams/Handlers is not enough, developers can define
Streams/Handlers with advanced flow controls, like these resilience Streams/Handlers
defined in this package

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
