# Document is a work-in-progress

## Go-gentle
[![Build
Status](https://travis-ci.org/cfchou/go-gentle.png?branch=master)](https://travis-ci.org/cfchou/go-gentle)

Talk to external services like a gentleman.

## Intro
Package gentle defines __Stream__ and __Handler__ interfaces and provides composable resilient implementations of them.

## Stream, Handler and back-pressure
__Stream__ and __Handler__ are our fundamental abstractions to achieve back-pressure. [Stream has Get()](https://godoc.org/github.com/cfchou/go-gentle/gentle#Stream) that emits Messages. [Handler has Handle()](https://godoc.org/github.com/cfchou/go-gentle/gentle#Handler) that transforms given Messages. They are composed altogether as one Stream which could then be pulled by the application's main loop.

## Resiliency
Resiliency patterns are indispensable in distributed systems because external services are not reliable at all time. We provide some useful patterns in the forms of Streams/Handlers. They include rate-limiting([NewRateLimitedStream](https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRateLimitedStream), [NewRateLimitedHandler](https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRateLimitedHandler)), retry/back-off([NewRetryStream](https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRetryStream), [NewRetryHanddler](https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRetryHandler)), bulkhead([NewBulkheadStream](https://godoc.org/github.com/cfchou/go-gentle/gentle#NewBulkheadStream), [NewBulkheadHandler](https://godoc.org/github.com/cfchou/go-gentle/gentle#NewBulkheadHandler)) and circuit-breaker([NewCircuitBreakerStream](https://godoc.org/github.com/cfchou/go-gentle/gentle#NewCircuitBreakerStream), [NewCircuitBreakerHandler](https://godoc.org/github.com/cfchou/go-gentle/gentle#NewCircuitBreakerHandler)). Each of them can be freely composed with other Streams/Handlers as one sees fit.

## Composability
Developers should implement their own logic in the forms of Streams/Handlers and then
compose them, possibly with our resilient counterpart. This package provides basic helpers like [AppendHandlersStream](https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendHandlersStream), [AppendHandlersHandler](https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendHandlersHandler), etc, to simplify composition.

If simply appending Streams/Handlers is not enough, developers can define Streams/Handlers
with advanced flow controls, like these resilience Streams/Handlers provided in this package.

## Document
[GoDoc](https://godoc.org/github.com/cfchou/go-gentle/gentle)

## Install

The master branch is considered unstable. Use [semantic versioning](http://gopkg.in/cfchou/go-gentle.v1/gentle) and verdor this library.

If you're using [glide](https://glide.sh/), simply run:
```
glide get gopkg.in/cfchou/go-gentle.v2
glide update
```

If you're not using package management tools, then
```
go get http://gopkg.in/cfchou/go-gentle.v2/gentle
```



