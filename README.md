# Document is a work-in-progress

## Go-gentle
[![GoDoc](https://godoc.org/github.com/cfchou/go-gentle/gentle?status.svg)](https://godoc.org/github.com/cfchou/go-gentle/gentle) [![Build Status](https://travis-ci.org/cfchou/go-gentle.png?branch=master)](https://travis-ci.org/cfchou/go-gentle) [![Go Report](https://goreportcard.com/badge/gopkg.in/cfchou/go-gentle.v3)](https://goreportcard.com/report/gopkg.in/cfchou/go-gentle.v3) [![Coverage Status](https://coveralls.io/repos/github/cfchou/go-gentle/badge.svg?branch=master)](https://coveralls.io/github/cfchou/go-gentle?branch=master)

Talk to external services like a gentleman.

## Intro
Package __gentle__ defines __Stream__ and __Handler__ interfaces and provides
composable resilient implementations(conveniently called __gentle-ments__).

Package __extra__  provides supplement components for __gentle__.


## Example

Error handling is omitted for brevity.

```
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
```

For not overwhelming the score-query service and the database, we use
__gentle-ments__(resilience Streams/Handlers defined in package gentle).

```
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
```

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

