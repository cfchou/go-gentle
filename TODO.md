## TODO
* Rename Bulkhead. Currently it employs semaphore isolation, cause the calling thread
    executes the client libs/backends)
    https://github.com/Netflix/Hystrix/wiki/How-it-Works#Isolation
    https://stackoverflow.com/questions/30391809/what-is-bulkhead-pattern-used-by-hystrix

* Remove Semaphore because it blocks so it's not resilient.
    or move it to package extra

* golint

* provide mocks in package extra



* RateLimit interface
    add https://godoc.org/golang.org/x/time/rate implementation
    may consider integrate with context
    add mock

* Add context support. Bulkhead can be thread-isolation.


* eliminate duplicated log.Error

* remove/export noopLogger

* improve Observe(value, labels, optional data)
    remove Unused MX_XXXX


## DONE
* Opts passed by addr
* Make XXXStream/Handler xXXStgream/Handler
* NewMockHandler/Stream(*mock.Mock)

## Ref
 https://github.com/golang/go/wiki/CodeReviewComments#pass-values
 https://github.com/golang/go/wiki/CodeReviewComments#interfaces
 https://github.com/golang/lint/issues/210
