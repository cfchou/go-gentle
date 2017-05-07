package main

import (
	"fmt"
	"golang.org/x/oauth2"
	"github.com/cfchou/go-gentle/gentle"
	mx "github.com/cfchou/go-gentle/extra/metrics_statsd"
	"github.com/cfchou/go-gentle/example/util"
	log15 "gopkg.in/inconshreveable/log15.v2"
	"os"
	"time"
	"github.com/spf13/pflag"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/rs/xid"
	"github.com/afex/hystrix-go/hystrix"
	"sync"
	"sync/atomic"
)

const (
	app_secret_file = "app_secret.json"
	namespace = "app"
)
var (
	logHandler = log15.MultiHandler(log15.StdoutHandler,
		log15.Must.FileHandler("./test.log", log15.JsonFormat()))
	log    = log15.New("mixin", "main")
	// command line options for server
	serverMaxDownloadsSec        = pflag.Int("server-max-downloads-sec",
		10, "downloads max number of mails in a sec")
	serverMaxConcurrentDownloads = pflag.Int("server-max-concurrent-downloads",
		2, "max concurrent downloads")

	// command line options for clients
	clientConcurrent   = pflag.Int("client-concurrenct",
		30, "number of concurrent clients")
	clientMaxDownloads = pflag.Int("client-max-downloads",
		1000, "max number of mails to download")

	statsdAddr = pflag.String("statsd-addr", "localhost:8125", "statsd addr")
	rateLimitInterval time.Duration

	// metrics
	statsdClient statsd.Statter
	mxStatter statsd.SubStatter
)

func init() {
	pflag.Parse()
	var err error
	statsdClient, err = statsd.NewClient(*statsdAddr, "")
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	rateLimitInterval = util.FreqToIntervalMillis(*serverMaxDownloadsSec)

	mxStatter = statsdClient.NewSubStatter("")
}

// NewUpstream puts up a upstream. It is likely to hit errors like:
// gentle.Err*, errors from the gentle library.
// util.ErrEOF, where no more messages is available.
// util.ErrNon2xx, could be:
// googleapi: Error 429: Too many concurrent requests for user, rateLimitExceeded
// googleapi: Error 429: User-rate limit exceeded.  Retry after 2017-03-13T19:26:54.011Z, rateLimitExceeded
func NewUpstream(appConfig *oauth2.Config, userTok *oauth2.Token) gentle.Stream {
	// Register metrics for some components.
	// They have to be done before components created.
	mx.RegisterRateLimitedHandlerMetrics(mxStatter, namespace, "")
	// histogram of numbers of retries
	mx.RegisterRetryHandlerMetrics(mxStatter, namespace, "")
	mx.RegisterHandlerStreamMetrics(mxStatter, namespace, "")
	mx.RegisterBulkheadHandlerMetrics(mxStatter, namespace, "")

	util.StatsdMetrics.RegisterGmailListStreamMetrics(mxStatter, namespace, "")
	util.StatsdMetrics.RegisterGmailMessageHandlerMetrics(mxStatter, namespace, "")

	// Enumerating message ids is pretty lite-weight.
	stream := util.NewGmailListStream(appConfig, userTok,
		namespace, "", 500)

	// Downloading emails is rate limited and retries in an unfortunate
	// event of error.
	handler := gentle.NewRetryHandler(namespace, "",
		gentle.NewRateLimitedHandler(namespace, "",
			util.NewGmailMessageHandler(appConfig, userTok,
				namespace, ""),
			gentle.NewTokenBucketRateLimit(rateLimitInterval,1)),
		[]time.Duration{
			20 * time.Millisecond,
			40 * time.Millisecond,
			80 * time.Millisecond,
		})

	return gentle.NewBulkheadStream(namespace, "",
		gentle.NewHandlerStream(namespace, "", stream, handler),
		*serverMaxConcurrentDownloads)
}

func run(appConfig *oauth2.Config, userTok *oauth2.Token) {
	begin := time.Now()
	upstream := NewUpstream(appConfig, userTok)

	conf := gentle.GetHystrixDefaultConfig()
	// We want no limit on concurrency here. But hystrix initialize a pool
	// by creating MaxConcurrentRequests tickets. If MaxConcurrentRequests
	// is extremely large then accessing the pool takes a long time.
	conf.MaxConcurrentRequests = 1024
	if *clientConcurrent > 1024 {
		conf.MaxConcurrentRequests = *clientConcurrent
	}
	conf.Timeout = 5000
	conf.SleepWindow = 5000
	conf.RequestVolumeThreshold = 10
	conf.ErrorPercentThreshold = 50

	// Set up a circuit for clients.
	circuit := xid.New().String()
	hystrix.ConfigureCommand(circuit, *conf)

	// Register metrics for the circuit-breaker
	// It has to be done before components created.
	mx.RegisterCircuitBreakerHandlerMetrics(mxStatter, namespace, "")

	stream := gentle.NewCircuitBreakerStream(namespace, "", upstream,
		circuit)

	var total_size int64
	var downloaded int64
	wg := sync.WaitGroup{}
	wg.Add(*clientConcurrent)

	var seenEOF int32
	clientFunc := func() {
		defer wg.Done()
		for {
			msg, err := stream.Get()
			if err != nil {
				log.Error("[main] Get() err", "err", err)
				if err == util.ErrEOF {
					// The first go-routine encountering
					// ErrEOF would exist immediately.
					// Others at some point would see
					// ErrCbOpen.
					atomic.StoreInt32(&seenEOF, 1)
					return
				}
				if atomic.LoadInt32(&seenEOF) == 1 {
					return
				}
				if err == gentle.ErrCbOpen {
					time.Sleep(gentle.IntToMillis(conf.SleepWindow))
				}
				continue
			}
			log.Debug("[main] Get() ok", "msg", msg.Id())
			gmsg := msg.(*util.GmailMessage).Msg
			n := atomic.AddInt64(&downloaded, 1)
			if n > int64(*clientMaxDownloads) {
				atomic.AddInt64(&downloaded, -1)
				break
			}
			atomic.AddInt64(&total_size, gmsg.SizeEstimate)
		}
	}

	for i := 0; i < *clientConcurrent; i++ {
		go clientFunc()
	}
	wg.Wait()
	log.Info("[main] Done", "downloaded", downloaded,
		"total_size", total_size,
		"timespan", time.Now().Sub(begin).Seconds())
}

func main() {
	defer statsdClient.Close()
	gentle.Log.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, logHandler))
	util.Log.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, logHandler))
	log.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, logHandler))

	config := util.GetAppSecret(app_secret_file)
	tok := util.GetTokenFromWeb(config)

	go run(config, tok)

	var blocked chan *struct{}
	<-blocked
}
