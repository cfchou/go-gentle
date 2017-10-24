/*
Package gentle defines Stream and Handler interfaces and provides composable
resilient implementations of them.


Stream and Handler

Stream and Handler are our fundamental abstractions. Stream has Get() that emits
Messages. Handler has Handle() that transforms given Messages.

  Message(https://godoc.org/github.com/cfchou/go-gentle/gentle#Message)
  Stream(https://godoc.org/github.com/cfchou/go-gentle/gentle#Stream)
  Handler(https://godoc.org/github.com/cfchou/go-gentle/gentle#Handler)

Developers should implement their own logic in the forms of Stream/Handler.
For simple cases, named types SimpleStream and SimpleHandler help directly
make a function a Stream/Handler.

  SimpleStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#SimpleStream)
  SimpleHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#SimpleHandler)

Example:

	// GameScore implements gentle.Message interface
	type GameScore struct {
		id string // better to be unique for tracing it in log
		Score int
	}

	// a gentle.Message must support ID
	func (s GameScore) ID() string {
		return s.id
	}

	// scoreStream is a gentle.Stream that wraps an API call to an external service for
	// getting game scores.
	// For simple cases that the logic can be defined entirely in a function, we can
	// simply define it to be a gentle.SimpleStream.
	var scoreStream gentle.SimpleStream = func(_ context.Context) (gentle.Message, error) {
		// simulate a result from an external service
		return &GameScore{
			id: "",
			Score: rand.Intn(100),
		}, nil
	}

	// DbWriter is a gentle.Handler that writes scores to the database.
	// Instead of using gentle.SimpleHandler, we define a struct explicitly
	// implementing gentle.Handler interface.
	type DbWriter struct {
		db *sql.DB
		table string
	}

	func (h *DbWriter) Handle(_ context.Context, msg gentle.Message) (gentle.Message, error) {
		gameScore := msg.(*GameScore)
		statement := fmt.Sprintf("INSERT INTO %s (score, date) VALUES (?, DATETIME());", h.table)
		_, err := h.db.Exec(statement, gameScore.Score)
		if err != nil {
			return nil, err
		}
		return msg, nil
	}

	// example continues in the next section

Gentle-ments -- our resilience Streams and Handlers

Resiliency patterns are indispensable in distributed systems because external
services are not always reliable. Some useful patterns in the forms of
Streams/Handlers are provided in this package(pun to call them gentle-ments).
They include rate-limiting, retry(back-off), bulkhead and circuit-breaker.
Each of them can be freely composed with other Streams/Handlers as one sees fit.

  RateLimitedStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#RateLimitedStream)
  RetryStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#RetryStream)
  BulkheadStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#BulkheadStream)
  CircuitStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#CircuitStream)

  RateLimitedHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#RateLimitedHandler)
  RetryHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#RetryHandler)
  BulkheadHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#BulkheadHandler)
  CircuitHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#CircuitHandler)

Generally, users call one of the option constructors like NewRetryHandlerOpts,
to get an option object filled with default values which can be mutated if necessary.
Then, pass it to one of the gentle-ment constructors above.

Example cont.(error handling is omitted for brevity):

	func main() {
		db, _ := sql.Open("sqlite3", "scores.sqlite")
		defer db.Close()
		db.Exec("DROP TABLE IF EXISTS game;")
		db.Exec("CREATE TABLE game (score INTEGER, date DATETIME);")

		dbWriter := &DbWriter{
			db: db,
			table: "game",
		}

		// Rate-limit the queries while allowing burst of some
		gentleScoreStream := gentle.NewRateLimitedStream(
			gentle.NewRateLimitedStreamOpts("myApp", "rlQuery",
				gentle.NewTokenBucketRateLimit(500*time.Millisecond, 5)),
			scoreStream)

		// Limit concurrent writes to Db
		limitedDbWriter := gentle.NewBulkheadHandler(
			gentle.NewBulkheadHandlerOpts("myApp", "bkWrite", 16),
			dbWriter)

		// Constantly backing off when limitedDbWriter returns an error
		backoffFactory := gentle.NewConstBackOffFactory(
			gentle.NewConstBackOffFactoryOpts(500*time.Millisecond, 5*time.Minute))
		gentleDbWriter := gentle.NewRetryHandler(
			gentle.NewRetryHandlerOpts("myApp", "rtWrite", backoffFactory),
			limitedDbWriter)

		// Compose the final Stream
		stream := gentle.AppendHandlersStream(gentleScoreStream, gentleDbWriter)

		// Keep fetching scores from the remote service to our database.
		// The amount of simultaneous go-routines are capped by the size of ticketPool.
		ticketPool := make(chan struct{}, 1000)
		for {
			ticketPool <- struct{}{}
			go func() {
				defer func(){ <-ticketPool }()
				stream.Get(context.Background())
			}()
		}
	}

Full example(https://gist.github.com/c2ac4060aaf0fcada38a3d85b3c07a71)

Convention

Throughout the package, we follow a convention to create Streams/Handlers or
other constructs. Firstly, we calling NewXxxOpts() to obtain an option object
initialized with default values. An option object is open for mutation. Once its
values are settled, we'll pass it to the corresponding constructor of gentle-ments.

Note that generally an option object is one-off. That is, it should not be reused
to construct more than one gentle-ment instance.

Composability

Like gentle-ments, users may define Streams/Handlers to compose other ones the
way they want. For simple cases, there are helpers for chaining Streams/Handlers.
Their semantic is that any failing element in the chain would skip the rest of all.
Also note that any element can also be a nested chain itself.

  AppendHandlersStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendHandlersStream)
  AppendHandlersHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendHandlersHandler)

We also define types of fallback and related helpers for appending a chain of
fallbacks to Stream/Handler. Any successful fallback in the chain would skip the
rest of all.

  StreamFallback(https://godoc.org/github.com/cfchou/go-gentle/gentle#StreamFallback)
  AppendFallbacksStream(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendFallbacksStream)

  HandlerFallback(https://godoc.org/github.com/cfchou/go-gentle/gentle#HandlerFallback)
  AppendFallbacksHandler(https://godoc.org/github.com/cfchou/go-gentle/gentle#AppendFallbacksHandler)

Context Support

Stream.Get() and Handler.Handle() both take context.Context. One of Context's common
usages is to collaborate request-scoped timeout. Our gentle-ments respect timeout
as much as possible and loyally pass the context to the user-defined upstreams
or up-handlers which may also respect context's timeout.

Thread Safety

Stream.Get() and Handler.Handle() should be thread-safe. A good practice is to
make Stream/Handler state-less. A Message needs not to be immutable but it's
good to be so. That said, gentle-ments' Get()/Handle() are all thread-safe and
don't mutate Messages.

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
