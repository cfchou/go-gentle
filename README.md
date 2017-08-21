# Document is a work-in-progress

## Go-gentle
[![GoDoc](https://godoc.org/gopkg.in/cfchou/go-gentle.v3/gentle?status.svg)](https://godoc.org/gopkg.in/cfchou/go-gentle.v3/gentle) [![Build Status](https://travis-ci.org/cfchou/go-gentle.png?branch=master)](https://travis-ci.org/cfchou/go-gentle) [![Go Report](https://goreportcard.com/badge/gopkg.in/cfchou/go-gentle.v3)](https://goreportcard.com/report/gopkg.in/cfchou/go-gentle.v3) [![Coverage Status](https://coveralls.io/repos/github/cfchou/go-gentle/badge.svg?branch=master)](https://coveralls.io/github/cfchou/go-gentle?branch=master)

Talk to external services like a gentleman.

## Intro
Package gentle defines __Stream__ and __Handler__ interfaces and provides composable resilient implementations of them.

## Example

Error handling is omitted for brevity.

```
// GameScore implements gentle.Message interface
type GameScore struct {
    id string // better to be unique for tracing its log
    score int
}

func (s GameScore) ID() string {
    return s.id
}

func parseGameScore(bs []byte) *GameScore {
    // ...
}

// query a restful api to get game score
var query gentle.SimpleStream = func(_ context.Context) (gentle.Message, error) {
    resp, _ := http.Get("https://get_game_score_api")
    defer resp.Body.Close()
    score := parseGameScore(resp.Body)
    return score, nil
}

// save game score
var writeDb gentle.SimpleHandler = func(_ context.Context, msg gentle.Message) (gentle.Message, error) {
    score := strconv.Itoa(msg.(*GameScore).score)
    db, _ := sql.Open("mysql", "user:password@tcp(127.0.0.1:3306)/hello")
    defer db.Close()
    stmt, _ := db.Prepare("UPDATE games SET score = $1 WHERE name = mygame")
    stmt.Exec(score)
    return msg, nil
}

// rate-limit the queries while allowing burst
gentleQuery := gentle.NewRateLimitedStream(
    gentle.NewRateLimitedStreamOpts("", "myApp",
        gentle.NewTokenBucketRateLimit(300*time.Millisecond, 5)),
    gentleQuery)

// limit concurrent writeDb
gentleWriteDb := gentle.NewBulkheadHandler(
    gentle.NewBulkheadHandlerOpts("", "myApp", 16),
    writeDb)

stream := gentle.AppendHandlersStream(gentleQuery, gentleWriteDb)

http.Handle("/refresh", func(w http.ResponseWriter, r *http.Request) {
    ...
    msg, err := stream.Get(r.context)
    ...
})

http.ListenAndServe(":12345", nil)
```

## Install

The master branch is considered unstable. Always depend on [semantic versioning](http://semver.org/) and verdor this library.

If you're using [glide](https://glide.sh/), simply run:
```
glide get gopkg.in/cfchou/go-gentle.v3/gentle
glide update
```

If you're not using package management tools, then
```
go get http://gopkg.in/cfchou/go-gentle.v3/gentle
```



