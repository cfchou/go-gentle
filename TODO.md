## TODO

* golint

* RateLimit interface
  * add https://godoc.org/golang.org/x/time/rate implementation
  * RateLimit.Wait() support context for supporting cancellation.

* Eliminate duplicated log. logging levels in Get()/Handle():
  * info: entry(for auditing)
  * debug: success return
  * error: failure return
  * warn: context.Done()
  * debug: others

* Improve API for CircuitBreaker config, register, flush.

* Reduce dependencies(exported symbols) to external packages like clock.

* Alternative circuitbreaker implementation https://github.com/rubyist/circuitbreaker

* metrics using expvar 

## Done
* Opts passed by addr
* Make Xxxxtream/Handler xXXXStgream/Handler
* NewMockHandler/Stream(*mock.Mock)
* Remove Semaphore because it blocks so it's not resilient. Or move it to package extra
* Improve Observe(value, labels, optional data). remove Unused MX_XXXX.
* Remove/export noopLogger
* Package-wise setLogger()
* opentracing
  * Span an be obtain from context.
  * Abstract logger to optionally writes to SpanContext.
* Add context support.
  * context should not be embedded in Message, since context propagation is normally one-way(parent to children).


## Dropped
* Rename Bulkhead. Currently it employs semaphore isolation, because the calling
  thread executes the client libs/backends
  * https://github.com/Netflix/Hystrix/wiki/How-it-Works#Isolation
  * https://stackoverflow.com/questions/30391809/what-is-bulkhead-pattern-used-by-hystrix
* Have a look at https://github.com/trustmaster/goflow. Inter-op?

## Ref
* https://github.com/golang/go/wiki/CodeReviewComments#pass-values
* https://github.com/golang/go/wiki/CodeReviewComments#interfaces
* https://github.com/golang/lint/issues/210
