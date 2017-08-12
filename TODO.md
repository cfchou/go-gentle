## TODO
* Rename Bulkhead. Currently it employs semaphore isolation, because the calling
    thread executes the client libs/backends
    https://github.com/Netflix/Hystrix/wiki/How-it-Works#Isolation
    https://stackoverflow.com/questions/30391809/what-is-bulkhead-pattern-used-by-hystrix

* golint

* Add context support.
    context should not be embedded in Message, since context propagation is
    normally one-way(parent to children).

* RateLimit interface
    add https://godoc.org/golang.org/x/time/rate implementation
    RateLimit.Wait() support context for supporting cancellation.

* opentracing
    Span an be obtain from context.
    Abstract logger to optionally writes to SpanContext.

* eliminate duplicated log.Error

* remove/export noopLogger

* improve Observe(value, labels, optional data)
    remove Unused MX_XXXX


## DONE
* Opts passed by addr
* Make XXXStream/Handler xXXStgream/Handler
* NewMockHandler/Stream(*mock.Mock)
* Remove Semaphore because it blocks so it's not resilient.
    or move it to package extra


## Ref
 https://github.com/golang/go/wiki/CodeReviewComments#pass-values
 https://github.com/golang/go/wiki/CodeReviewComments#interfaces
 https://github.com/golang/lint/issues/210
