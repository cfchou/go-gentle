## TODO

* golint

* Add context support.
  * context should not be embedded in Message, since context propagation is normally one-way(parent to children).

* RateLimit interface
  * add https://godoc.org/golang.org/x/time/rate implementation
  * RateLimit.Wait() support context for supporting cancellation.

* opentracing
  * Span an be obtain from context.
  * Abstract logger to optionally writes to SpanContext.

* Eliminate duplicated log. logging levels in Get()/Handle():
  * info: entry(for auditing)
  * debug: success return
  * error: failure return
  * warn: context.Done()
  * debug: others

* Remove/export noopLogger

* Package-wise setLogger()

* Improve Observe(value, labels, optional data). remove Unused MX_XXXX.

* Exported symbols make clients also depend on external packages like clock.

* Alternative circuitbreaker implementation https://github.com/rubyist/circuitbreaker

## Done
* Opts passed by addr
* Make XXXStream/Handler xXXStgream/Handler
* NewMockHandler/Stream(*mock.Mock)
* Remove Semaphore because it blocks so it's not resilient.
    or move it to package extra

## Dropped
* Rename Bulkhead. Currently it employs semaphore isolation, because the calling
  thread executes the client libs/backends
  * https://github.com/Netflix/Hystrix/wiki/How-it-Works#Isolation
  * https://stackoverflow.com/questions/30391809/what-is-bulkhead-pattern-used-by-hystrix

## Ref
* https://github.com/golang/go/wiki/CodeReviewComments#pass-values
* https://github.com/golang/go/wiki/CodeReviewComments#interfaces
* https://github.com/golang/lint/issues/210
