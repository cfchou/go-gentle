/*
Package gentle defines Stream and Handler interfaces and provides composable
resilient implementations of them.


Stream and Handler

Stream and Handler are our fundamental abstractions to achieve back-pressure.
Stream has Get() that emits Messages. Handler has Handle() that transforms
given Messages.

  Stream(https://godoc.org/github.com/cfchou/go-gentle/gentle#Stream)
  Handler(https://godoc.org/github.com/cfchou/go-gentle/gentle#Handler)

  // query a restful api
  var query gentle.SimpleStream = func(_ context.Context) (gentle.Message, error) {
  	resp, err := http.Get("https://some_api_endpoint")
  	if err != nil {
  		return nil, err
  	}
  	defer resp.Body.Close()
  	content, err := ioutil.ReadAll(resp.Body)
  	if err != nil {
  		return nil, err
  	}
  	return gentle.SimpleMessage(string(content)), nil
  }

  var writeToDb gentle.SimpleHandler = func(_ context.Context, msg gentle.Message) (gentle.Message, error) {
	db, err := sql.Open("mysql", "user:password@tcp(127.0.0.1:3306)/hello")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	stmt, err := db.Prepare("INSERT INTO game(score) VALUES(?)")
	if err != nil {
		return nil, err
	}
	content := string(msg.(gentle.SimpleMessage))
	_, err = stmt.Exec(content)
	if err != nil {
		return nil, err
	}
	return msg, nil
  }

  stream := gentle.AppendHandlersStream(query, writeToDb)

  for {
  	_, err := stream.Get(context.Background())
  	if err != nil {
  		fmt.Println(err)
	}
	// we will get rid of this
	time.sleep(300*time.Millisecond)
  }

Developers should implement their own logic in the forms of Stream/Handler.
For simple cases, named types SimpleStream and SimpleHandler help to directly
use a function as a Stream/Handler.

  SimpleStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#SimpleStream)
  SimpleHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#SimpleHandler)

Our Resilience Streams and Handlers

Resiliency patterns are indispensable in distributed systems because external
services are not always reliable. Some useful patterns in the forms of
Streams/Handlers are provided in this package. They include rate-limiting,
retry(also known as back-off), bulkhead and circuit-breaker. Each of them can be
freely composed with other Streams/Handlers as one sees fit.

  NewRateLimitedStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRateLimitedStream)
  NewRetryStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRetryStream)
  NewBulkheadStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewBulkheadStream)
  NewCircuitStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewCircuitStream)

  NewRateLimitedHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRateLimitedHandler)
  NewRetryHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRetryHandler)
  NewBulkheadHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewBulkheadHandler)
  NewCircuitHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewCircuitHandler)

  // rate-limit the queries, allow burst
  rlQuery := gentle.NewRateLimitedStream(
  	gentle.NewRateLimitedStreamOpts("", "myApp",
  		gentle.NewTokenBucketRateLimit(300*time.Millisecond, 5)),
  	query)

  // limit concurrent writeToDb
  gentleWrite := gentle.NewBulkheadHandler(
  	gentle.NewBulkheadHandlerOpts("", "myApp", 16),
	writeToDb)

  stream := gentle.AppendHandlersStream(rlQuery, exclusiveAppend)

  for {
  	_, err := stream.Get(context.Background())
  	if err != nil {
  		fmt.Println(err)
	}
	// we don't sleep anymore while not overwhelming query and writeToDb
  }


Composability

A Stream/Handler can chain with an arbitrary number of Handlers. Their semantic
is that any failing element in the chain would skip the rest of all. Also note
that any element can also be a nested chain itself.

  AppendHandlersStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendHandlersStream)
  AppendHandlersHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendHandlersHandler)

There are also helpers for chaining fallbacks.

  AppendFallbacksStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendFallbacksStream)
  AppendFallbacksHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendFallbacksHandler)

If those helpers are not enough, developers can define Streams/Handlers with
advanced flow controls, like how we define resilience Streams/Handlers.

Note

The implementations of Stream.Get() and Handler.Handle() should be thread-safe.
A good practice is to make Stream/Handler state-less. A Message needs not to be
immutable but it's good to be so. That said, our resilience Streams/Handlers are
all thread-safe and don't mutate Messages.

Stream.Get() and Handler.Handle() both take context.Context. Context's common
usage is to achieve request-scoped timeout. Our resilience Streams/Handlers
respect timeout as much as possible and loyally pass the context to the
user-defined upstreams or up-handlers which should also respect context's
timeout.

External References

Some of our implementations make heavy use of third-party packages. It helps to
checkout their documents.

  Circuit-breaker is based on hystrix-go(https://godoc.org/github.com/afex/hystrix-go/hystrix).
  Rate-limiting is based on juju/ratelimit(https://godoc.org/github.com/juju/ratelimit).
  Logging is based on log15(https://godoc.org/gopkg.in/inconshreveable/log15.v2).

*/
package gentle
