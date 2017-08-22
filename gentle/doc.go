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
For simple cases, named types SimpleStream and SimpleHandler help to directly
use a function as a Stream/Handler.

  SimpleStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#SimpleStream)
  SimpleHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#SimpleHandler)

Example(error handling is omitted for brevity):

  // GameScore implements gentle.Message interface
  type GameScore struct {
    id string // better to be unique for tracing the log
    score int
  }

  func (s GameScore) ID() string {
    return s.id
  }

  func parseGameScore(bs []byte) *GameScore {
    // ...
  }

  // query is a Stream that calls a restful api to get game score
  var query gentle.SimpleStream = func(_ context.Context) (gentle.Message, error) {
    resp, _ := http.Get("https://get_game_score_api")
    defer resp.Body.Close()
    score := parseGameScore(resp.Body)
    return score, nil
  }

  // writeDB is a Handler that saves game score to the database
  var writeDb gentle.SimpleHandler = func(_ context.Context, msg gentle.Message) (gentle.Message, error) {
    score := strconv.Itoa(msg.(*GameScore).score)
    db, _ := sql.Open("mysql", "user:password@tcp(127.0.0.1:3306)/hello")
    defer db.Close()
    stmt, _ := db.Prepare("UPDATE games SET score = $1 WHERE name = mygame")
    stmt.Exec(score)
    return msg, nil
  }

  // example continues in the next section

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

Example(cont.):

  // rate-limit the queries while allowing burst
  gentleQuery := gentle.NewRateLimitedStream(
    gentle.NewRateLimitedStreamOpts("", "myApp",
      gentle.NewTokenBucketRateLimit(300*time.Millisecond, 5)),
    query)

  // limit concurrent writeDb
  gentleWriteDb := gentle.NewBulkheadHandler(
    gentle.NewBulkheadHandlerOpts("", "myApp", 16),
    writeDb)

  stream := gentle.AppendHandlersStream(gentleQuery, gentleWriteDb)

  http.Handle("/refresh", func(w http.ResponseWriter, r *http.Request) {
    msg, err := stream.Get(r.context)
    ...
  })
  http.ListenAndServe(":12345", nil)


Composability

Users may define Streams/Handlers to compose other ones the way they want(like
how resilience Streams/Handlers are defined). For simple cases, there are helpers
for chaining Streams/Handlers. Their semantic is that any failing element in the
chain would skip the rest of all. Also note that any element can also be a
nested chain itself.

  AppendHandlersStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendHandlersStream)
  AppendHandlersHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendHandlersHandler)

There are also helpers for chaining fallbacks.

  AppendFallbacksStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendFallbacksStream)
  AppendFallbacksHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendFallbacksHandler)

Context Support

Stream.Get() and Handler.Handle() both take context.Context. Context's common
usage is to achieve request-scoped timeout. Our resilience Streams/Handlers
respect timeout as much as possible and loyally pass the context to the
user-defined upstreams or up-handlers which should also respect context's
timeout.

Thread Safety

User-defined Stream.Get() and Handler.Handle() should be thread-safe. A good
practice is to make Stream/Handler state-less. A Message needs not to be
immutable but it's good to be so. That said, our resilience Streams/Handlers are
all thread-safe and don't mutate Messages.

Logging

A logger must support the Logger interface.

There's a root logger gentle.Log. Moreover, every Stream/Handler has its own
logger. It can be get/set via its options. Conceptually, each of these loggers
is a child returned by gentle.Log.New(fields) where fields are key-value pairs:
 "namespace": "namespace of this Stream/Handler"
 "name": "name of this Stream/Handler"
 "gentle": "type of this Stream/Handler"

By default we use log15(https://godoc.org/gopkg.in/inconshreveable/log15.v2).
However, users may replace it with whatever library that supports Logger interface.
Check out examples(https://github.com/cfchou/go-gentle/blob/master/gentle/example_logger_test.go)
of how to use logrus logger.

Logger interface doesn't have methods like SetHandler or SetLevel, because
they are often implementation-dependent. Instead, you set up the logger and then
assign it to gentle.Log or XxxxxOpts.Log. That way, we have fine-grained control
over every Logger. Check out examples(https://github.com/cfchou/go-gentle/blob/master/gentle/example_logger_test.go)
of how to setup logging level of the default log15 loggers.


External References

Some of our implementations make heavy use of third-party packages. It helps to
checkout their documents.

  Circuit-breaker is based on hystrix-go(https://godoc.org/github.com/afex/hystrix-go/hystrix).
  Rate-limiting is based on juju/ratelimit(https://godoc.org/github.com/juju/ratelimit).

*/
package gentle
