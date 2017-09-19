## Go-gentle
[![GoDoc](https://godoc.org/github.com/cfchou/go-gentle/gentle?status.svg)](https://godoc.org/github.com/cfchou/go-gentle/gentle) [![Build Status](https://travis-ci.org/cfchou/go-gentle.png?branch=master)](https://travis-ci.org/cfchou/go-gentle) [![Go Report](https://goreportcard.com/badge/gopkg.in/cfchou/go-gentle.v3)](https://goreportcard.com/report/gopkg.in/cfchou/go-gentle.v3) [![Coverage Status](https://coveralls.io/repos/github/cfchou/go-gentle/badge.svg?branch=master)](https://coveralls.io/github/cfchou/go-gentle?branch=master)

Talk to external services like a gentleman.

## Intro
Package __gentle__ defines __Stream__ and __Handler__ interfaces and provides
composable resilient implementations(conveniently called __gentle-ments__).
Please refer to this [overview](https://godoc.org/github.com/cfchou/go-gentle/gentle/#pkg-overview). 

Package __extra__  provides supplement components(logger adaptors, metric collectors, etc.) for __gentle__.


## Example

Error handling is omitted for brevity.

```
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
```

For not overwhelming the score-query service and the database, we use
__gentle-ments__(resilience Streams/Handlers defined in package gentle).

```
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
		go stream.Get(context.Background())
		<-ticketPool
	}
}
```

[Full example](https://gist.github.com/c2ac4060aaf0fcada38a3d85b3c07a71)

## Install

The master branch is considered unstable. Always depend on [semantic versioning](http://semver.org/) and verdor this library.

If you're using [glide](https://glide.sh/), simply run:
```
glide get gopkg.in/cfchou/go-gentle.v3
glide update
```

If you're not using package management tools, then
```
go get gopkg.in/cfchou/go-gentle.v3
```


## Other features
* __Logging__: Gentle-ments log extensively. Users can plug in their logger library
of choice. Package _extra/log_ provides adaptors for [log15](https://github.com/inconshreveable/log15)
and [logrus](https://github.com/sirupsen/logrus).
* __Metrics__: Gentle-ments send metrics in aid of monitoring. Users can plug in
their metric collector of choice. Package _extra/metric_ provides collectors
of [prometheus](http://http://prometheus.io/) and [statsd](https://github.com/etsy/statsd/).
* __OpenTracing__: Gentle-ments integrates [OpenTracing](https://github.com/opentracing/opentracing-go).
Users can choose a variety of backends that support OpenTracing. Package
_extra/tracing_ provides an example of using Uber's [jaeger](https://github.com/uber/jaeger)
as the backend.

