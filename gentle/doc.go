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
		id string // better to be unique for tracing its log
		score int
	}

	// ID is the only method that a gentle.Message must have
	func (s GameScore) ID() string {
		return s.id
	}

	func parseGameScore(body io.Reader) *GameScore {
		// ...
	}

	// Implement a gentle.Stream which queries a restful api to get a game score.
	// For simple case like this, we can define the logic to be a gentle.SimpleStream.
	var query gentle.SimpleStream = func(_ context.Context) (gentle.Message, error) {
		resp, _ := http.Get("https://get_game_score_api")
		defer resp.Body.Close()
		score := parseGameScore(resp.Body)
		return score, nil
	}

	// Implement a gentle.Handler which saves game scores to a database. For simple
	// case like this, we can define the logic to be a gentle.SimpleHandler.
	var writeDb gentle.SimpleHandler = func(_ context.Context, msg gentle.Message) (gentle.Message, error) {
		score := strconv.Itoa(msg.(*GameScore).score)
		db, _ := sql.Open("mysql", "user:password@tcp(127.0.0.1:3306)/hello")
		defer db.Close()
		stmt, _ := db.Prepare("UPDATE games SET score = $1 WHERE name = mygame")
		stmt.Exec(score)
		return msg, nil
	}

	// example continues in the next section

Gentle-ments -- our resilience Streams and Handlers

Resiliency patterns are indispensable in distributed systems because external
services are not always reliable. Some useful patterns in the forms of
Streams/Handlers are provided in this package(pun to call them gentle-ments).
They include rate-limiting, retry(a.k.a back-off), bulkhead and circuit-breaker.
Each of them can be freely composed with other Streams/Handlers as one sees fit.

  NewRateLimitedStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRateLimitedStream)
  NewRetryStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRetryStream)
  NewBulkheadStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewBulkheadStream)
  NewCircuitStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewCircuitStream)

  NewRateLimitedHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRateLimitedHandler)
  NewRetryHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRetryHandler)
  NewBulkheadHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewBulkheadHandler)
  NewCircuitHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewCircuitHandler)

Generally, users call one of the option constructors like
NewRetryHandlerOpts(https://godoc.org/github.com/cfchou/go-gentle/gentle#NewRetryHandlerOpts),
to get an default option object which can be mutated for changing, for example
its logger. Then, pass it it to one of the gentle-ment constructors above.

Example(cont.):

	func main() {
		// Rate-limit the queries while allowing burst
		gentleQuery := gentle.NewRateLimitedStream(
			gentle.NewRateLimitedStreamOpts("myApp", "rlQuery",
				gentle.NewTokenBucketRateLimit(300*time.Millisecond, 5)),
				query)

		// Limit concurrent writeDb
		limitedWriteDb := gentle.NewBulkheadHandler(
			gentle.NewBulkheadHandlerOpts("myApp", "bkWrite", 16),
			writeDb)

		// Constantly backing off when limitedWriteDb returns ErrMaxConcurrency
		backoffFactory := gentle.NewConstBackOffFactory(
			gentle.NewConstBackOffFactoryOpts(500*time.Millisecond, 5*time.Minute))
		gentleWriteDb := gentle.NewRetryHandler(
			gentle.NewRetryHandlerOpts("myApp", "rtWrite", backoffFactory),
			limitedWriteDb)

		// Compose the final Stream
		stream := gentle.AppendHandlersStream(gentleQuery, gentleWriteDb)

		// Keep fetching scores from the remote service to our database.
		// The amount of simultaneous go-routines are capped by the size of ticketPool.
		ticketPool := make(chan struct{}, 1000)
		for {
			ticketPool <- struct{}{}
			go stream.Get(context.Background())
			<-ticketPool
		}
	}


Composability

Like gentle-ments, users may define Streams/Handlers to compose other ones the
way they want. For simple cases, there are helpers
for chaining Streams/Handlers. Their semantic is that any failing element in the
chain would skip the rest of all. Also note that any element can also be a
nested chain itself.

  AppendHandlersStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendHandlersStream)
  AppendHandlersHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendHandlersHandler)

There are also helpers for chaining fallbacks with different semantics.

  AppendFallbacksStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendFallbacksStream)
  AppendFallbacksHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendFallbacksHandler)

Context Support

Stream.Get() and Handler.Handle() both take context.Context. Context's common
usage is to collaborate request-scoped timeout. Our gentle-ments respect timeout
as much as possible and loyally pass the context to the user-defined upstreams
or up-handlers which should also respect context's timeout.

Thread Safety

Stream.Get() and Handler.Handle() should be thread-safe. A good
practice is to make Stream/Handler state-less. A Message needs not to be
immutable but it's good to be so. That said, gentle-ments' Get()/Handle() are
all thread-safe and don't mutate Messages.

Logging

Users may plug in whatever logging library they like as long as it supports
interface Logger(https://godoc.org/github.com/cfchou/go-gentle/gentle#Logger).
Fans of log15 and logurs may check out the sibling package
extra/log(https://godoc.org/github.com/cfchou/go-gentle/extra/log) for
adapters already available at hand.

There's a root logger gentle.Log which if not specified is a no-op logger.
Every gentle-ment has its own logger. Users can get/set the logger in the option
object which is then be used to initialize a gentle-ment. By default, each of
these loggers is a child returned by gentle.Log.New(fields) where fields are
key-value pairs of:
 "namespace": "namespace of this Stream/Handler"
 "name": "name of this Stream/Handler"
 "gentle": "type of this Stream/Handler"

Logger interface doesn't have methods like SetHandler or SetLevel, because
such functions are often implementation-dependent. Instead, you set up the
logger and then assign it to gentle.Log or a gentle-ment's option. That way, we
have fine-grained controls over logging. Check out the sibling package extra/log
for examples.

Metrics

Currently there're three metric interfaces of metrics collectors for gentle-ments:

  Metric for RateLimitedStream/Handler, BulkheadStream/Handler(https://godoc.org/github.com/cfchou/go-gentle/gentle#Metric)
  RetryMetric for RetryStream/Handler(https://godoc.org/github.com/cfchou/go-gentle/gentle#RetryMetric)
  CbMetric for CircuitStream/Handler(https://godoc.org/github.com/cfchou/go-gentle/gentle#CbMetric)

In the sibling package extra/metric(https://godoc.org/github.com/cfchou/go-gentle/extra/metric),
we have provided implementations for prometheus and statsd and examples.
Generally, it's similar to Logger in that one can change an option's metrics
collector before creating a gentle-ment. By default, metrics collectors are all
no-op.

OpenTracing

Gentle-ments integrate OpenTracing(https://github.com/opentracing/opentracing-go).
Users may create a span in the root context which is then passed around by
Streams/Handlers. Gentle-ments' options come with a opentracing.Tracer which is
by default a global tracer. There's an example of using Uber's jaeger as the
backend(https://github.com/cfchou/go-gentle/blob/master/extra/tracing/jaeger.go).


*/
package gentle
