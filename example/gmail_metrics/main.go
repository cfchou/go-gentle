package main

import (
	"fmt"
	"io/ioutil"

	"errors"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/googleapi"
	"github.com/cfchou/go-gentle/gentle"
	mp "github.com/cfchou/go-gentle/gentle/extra/metrics_prometheus"
	log15 "gopkg.in/inconshreveable/log15.v2"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"github.com/spf13/pflag"
)

const (
	app_secret_file = "app_secret.json"
)

var (
	logHandler = log15.MultiHandler(log15.StdoutHandler,
		log15.Must.FileHandler("./test.log", log15.LogfmtFormat()))
	log    = log15.New("mixin", "main")
	ErrEOF = errors.New("EOF")
	// command line options
	maxMails       = pflag.Int("max-mails", 1000, "max number of mails to download")
	maxConcurrency       = pflag.Int("max-concurrency", 300, "max concurrent")
	gmailCallsHist = prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: "gmailapi",
			Name:      "duration_seconds",
			Help:      "Duration of Gmail API in seconds",
			Buckets:   prom.DefBuckets,
		},
		[]string{"api", "status"})
	gmailMessageBytesTotalCounter = prom.NewCounter(
		prom.CounterOpts{
			Namespace: "gmailapi",
			Name: "gmail_message_bytes_total",
			Help: "Gmail raw messages size in total",
		})
)

func init() {
	pflag.Parse()
	prom.MustRegister(gmailCallsHist)
	prom.MustRegister(gmailMessageBytesTotalCounter)
}

// getTokenFromWeb uses Config to request a Token.
// It returns the retrieved Token.
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var code string
	if _, err := fmt.Scan(&code); err != nil {
		log.Error("Unable to read authorization code", "err", err)
		os.Exit(1)
	}

	tok, err := config.Exchange(context.TODO(), code)
	if err != nil {
		log.Error("Unable to retrieve token from web", "err", err)
		os.Exit(1)
	}
	return tok
}

func getAppSecret(file string) *oauth2.Config {
	bs, err := ioutil.ReadFile(file)
	if err != nil {
		log.Error("ReadFile err", "err", err)
		os.Exit(1)
	}

	config, err := google.ConfigFromJSON(bs, gmail.GmailReadonlyScope)
	if err != nil {
		log.Error("ConfigFromJson err", "err", err)
		os.Exit(1)
	}
	return config
}

type gmailMessage struct {
	msg *gmail.Message
}

func (m *gmailMessage) Id() string {
	return m.msg.Id
}

type gmailListStream struct {
	Log           log15.Logger
	service       *gmail.Service
	listCall      *gmail.UsersMessagesListCall
	lock          sync.Mutex
	messages      []*gmail.Message
	nextPageToken string
	page_num      int
	page_last     bool
	terminate     chan *struct{}
}

func NewGmailListStream(appConfig *oauth2.Config, userTok *oauth2.Token, max_results int64) *gmailListStream {
	client := appConfig.Client(context.Background(), userTok)
	// Timeout for a request
	client.Timeout = time.Second * 30

	service, err := gmail.New(client)
	if err != nil {
		log.Error("gmail.New err", "err", err)
		os.Exit(1)
	}

	listCall := service.Users.Messages.List("me")
	listCall.MaxResults(max_results)
	return &gmailListStream{
		Log:       log.New("mixin", "list"),
		service:   service,
		listCall:  listCall,
		lock:      sync.Mutex{},
		page_last: false,
		terminate: make(chan *struct{}),
	}
}

func (s *gmailListStream) nextMessage() (*gmailMessage, error) {
	// assert s.lock is already Locked
	if s.messages == nil || len(s.messages) == 0 {
		s.Log.Error("Invalid state")
		os.Exit(1)
	}
	msg := &gmailMessage{msg: s.messages[0]}
	s.messages = s.messages[1:]
	s.Log.Debug("List() nextMessge", "msg", msg.Id(), "page", s.page_num,
		"len_msgs_left", len(s.messages))
	return msg, nil
}

func (s *gmailListStream) Get() (gentle.Message, error) {
	s.Log.Debug("List() ...")
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.messages != nil && len(s.messages) > 0 {
		return s.nextMessage()
	}
	// Messages on this page are consumed, fetch next page
	if s.page_last {
		s.Log.Info("List() EOF, no more messages and pages")
		select {
		case <-s.terminate:
			return nil, ErrEOF
		}
	}
	if s.nextPageToken != "" {
		s.listCall.PageToken(s.nextPageToken)
	}
	callStart := time.Now()
	resp, err := s.listCall.Do()
	if err != nil {
		gmailCallsHist.With(
			prom.Labels{
				"api":    "list",
				"status": toGoogleApiErrorCode(err),
			}).Observe(time.Now().Sub(callStart).Seconds())
		s.Log.Error("List() err", "err", err)
		return nil, err
	}
	gmailCallsHist.With(
		prom.Labels{
			"api":    "list",
			"status": strconv.Itoa(resp.HTTPStatusCode),
		}).Observe(time.Now().Sub(callStart).Seconds())

	if resp.NextPageToken == "" {
		s.Log.Info("List() No more pages")
		s.page_last = true
	}

	s.messages = resp.Messages
	s.nextPageToken = resp.NextPageToken
	s.page_num++
	s.Log.Info("List() Read a page", "page", s.page_num,
		"len_msgs", len(s.messages), "nextPageToken", s.nextPageToken)
	if len(s.messages) == 0 {
		s.Log.Info("List() EOF, no more messages")
		select {
		case <-s.terminate:
			return nil, ErrEOF
		}
	}
	return s.nextMessage()
}

func toGoogleApiErrorCode(err error) string {
	if gerr, ok := err.(*googleapi.Error); ok {
		return strconv.Itoa(gerr.Code)
	}
	return err.Error()
}

type gmailMessageHandler struct {
	Log     log15.Logger
	service *gmail.Service
}

func NewGmailMessageHandler(appConfig *oauth2.Config, userTok *oauth2.Token) *gmailMessageHandler {
	client := appConfig.Client(context.Background(), userTok)
	//client.Timeout = time.Second * 30
	service, err := gmail.New(client)
	if err != nil {
		log.Error("gmail.New err", "err", err)
		os.Exit(1)
	}
	return &gmailMessageHandler{
		Log:     log.New("mixin", "download"),
		service: service,
	}
}

func (h *gmailMessageHandler) Handle(msg gentle.Message) (gentle.Message, error) {
	h.Log.Debug("Message.Get() ...", "msg_in", msg.Id())
	getCall := h.service.Users.Messages.Get("me", msg.Id())
	getCall.Format("raw")
	callStart := time.Now()
	gmsg, err := getCall.Do()
	if err != nil {
		gmailCallsHist.With(
			prom.Labels{
				"api":    "get",
				"status": toGoogleApiErrorCode(err),
			}).Observe(time.Now().Sub(callStart).Seconds())
		h.Log.Error("Messages.Get() err", "msg_in", msg.Id(),
			"err", err)
		return nil, err
	}
	gmailCallsHist.With(
		prom.Labels{
			"api":    "get",
			"status": strconv.Itoa(gmsg.HTTPStatusCode),
		}).Observe(time.Now().Sub(callStart).Seconds())
	gmailMessageBytesTotalCounter.Add(float64(gmsg.SizeEstimate))
	h.Log.Debug("Messages.Get() ok", "msg_in", msg.Id(),
		"msg_out", gmsg.Id, "size", gmsg.SizeEstimate)
	return &gmailMessage{msg: gmsg}, nil
}

func listStream(appConfig *oauth2.Config, userTok *oauth2.Token) gentle.Stream {
	// max. 500 mail ids per page
	stream := NewGmailListStream(appConfig, userTok, 500)
	stream.Log.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, logHandler))
	return stream
}

func example_hit_ratelimit(appConfig *oauth2.Config, userTok *oauth2.Token) gentle.Stream {
	lstream := listStream(appConfig, userTok)
	stream := gentle.NewMappedStream("gmail", "map1", lstream,
		NewGmailMessageHandler(appConfig, userTok))

	return stream
}

func example_ratelimited(appConfig *oauth2.Config, userTok *oauth2.Token) gentle.Stream {
	lstream := listStream(appConfig, userTok)
	handler := gentle.NewRateLimitedHandler("gmail",
		NewGmailMessageHandler(appConfig, userTok),
		// (1000/request_interval) messages/sec, but it's an upper
		// bound, the real speed is likely much lower.
		gentle.NewTokenBucketRateLimit(1, 1))

	stream := gentle.NewMappedStream("gmail", "map1", lstream, handler)
	stream.Log.SetHandler(logHandler)

	return stream
}

func example_ratelimited_retry(appConfig *oauth2.Config, userTok *oauth2.Token) gentle.Stream {
	lstream := listStream(appConfig, userTok)
	rhandler := gentle.NewRateLimitedHandler("gmail",
		NewGmailMessageHandler(appConfig, userTok),
		// (1000/request_interval) messages/sec, but it's an upper
		// bound, the real speed is likely much lower.
		gentle.NewTokenBucketRateLimit(1, 1))

	handler := gentle.NewRetryHandler("gmail", rhandler, []time.Duration{
		20 * time.Millisecond, 40 * time.Millisecond, 80 * time.Millisecond})
	handler.Log.SetHandler(logHandler)

	stream := gentle.NewMappedStream("gmail", "map1", lstream, handler)
	stream.Log.SetHandler(logHandler)
	return stream
}

type timedResult struct {
	msg  gentle.Message
	dura time.Duration
}

func RunWithBulkheadStream(upstream gentle.Stream, max_concurrency int, count int) {
	stream := gentle.NewBulkheadStream("gmail", "bulk1", upstream, max_concurrency)

	// total should be, if gmail Messages.List() doesn't return error,
	// the total of all gmailListStream emits pluses 1(ErrEOF).
	// total is no more than count.
	var total int64
	// success_total should be, the number of mails have been successfully
	// downloaded.
	success_total := 0
	var total_size int64

	result := make(chan *timedResult, max_concurrency)
	total_begin := time.Now()
	// Caller calls BulkheadStream.Get() concurrently.
	for i := 0; i < count; i++ {
		go func() {
			begin := time.Now()
			msg, err := stream.Get()
			atomic.AddInt64(&total, 1)
			if err != nil {
				log.Error("Get() err", "err", err)
				return
			}
			result <- &timedResult{
				msg:  msg,
				dura: time.Now().Sub(begin),
			}
		}()
	}
	mails := make(map[string]bool)
	tm := time.NewTimer(10 * time.Second)
	var total_end time.Time
LOOP:
	for {
		select {
		case tr := <-result:
			gmsg := tr.msg.(*gmailMessage).msg
			log.Debug("Got message", "msg", gmsg.Id,
				"size", gmsg.SizeEstimate)
			total_end = time.Now()
			success_total++
			total_size += gmsg.SizeEstimate
			// Test duplication
			if _, existed := mails[gmsg.Id]; existed {
				log.Error("Duplicated Messagge", "msg", gmsg.Id)
				break LOOP
			}
			mails[gmsg.Id] = true
			tm.Reset(10 * time.Second)
		case <-tm.C:
			log.Debug("break ...")
			break LOOP
		}
	}
	total_time := total_end.Sub(total_begin)
	log.Info("Done", "total", atomic.LoadInt64(&total), "success_total", success_total,
		"total_size", total_size,
		"total_time", total_time)
}

func RunWithConcurrentFetchStream(upstream gentle.Stream, max_concurrency int, count int) {
	stream := gentle.NewConcurrentFetchStream("gmail", "con1", upstream, max_concurrency)

	// total should be, if gmail Messages.List() doesn't return error,
	// the total of all gmailListStream emits pluses 1(ErrEOF).
	// total is no more than count.
	total := 0
	// success_total should be, the number of mails have been successfully
	// downloaded.
	success_total := 0
	var total_size int64

	total_begin := time.Now()
	var total_end time.Time
	mails := make(map[string]bool)
	// From caller's point of view, ConcurrentFetchStream.Get() is
	// called sequentially. But under the hood, there's a loop running in
	// parallel to asynchronously fetch Messages from upstream.
LOOP:
	for i := 0; i < count; i++ {
		// essentially sequencing stream.Get()
		result := make(chan interface{}, 1)
		tm := time.NewTimer(10 * time.Second)
		go func() {
			msg, err := stream.Get()
			if err != nil {
				log.Error("Get() err", "err", err)
				result <- err
			}
			result <- msg
		}()
		var v interface{}
		select {
		case v = <-result:
		case <-tm.C:
			log.Info("Get() timeout")
			break LOOP
		}
		// sequenced stream.Get() finished

		total += 1
		total_end = time.Now()
		if msg, ok := v.(gentle.Message); ok {
			success_total++
			gmsg := msg.(*gmailMessage).msg
			log.Debug("Got message", "msg", gmsg.Id,
				"size", gmsg.SizeEstimate)
			total_size += gmsg.SizeEstimate
			// Test duplication
			if _, existed := mails[gmsg.Id]; existed {
				log.Error("Duplicated Messagge", "msg", gmsg.Id)
				break
			}
			mails[gmsg.Id] = true
		}
	}
	total_time := total_end.Sub(total_begin)
	log.Info("Done", "total", total, "success_total", success_total,
		"total_size", total_size,
		"total_time", total_time)
}

func main() {
	h := log15.LvlFilterHandler(log15.LvlDebug, logHandler)
	log.SetHandler(h)
	config := getAppSecret(app_secret_file)
	tok := getTokenFromWeb(config)

	// likely to hit error like:
	// googleapi: Error 429: Too many concurrent requests for user, rateLimitExceeded
	// googleapi: Error 429: User-rate limit exceeded.  Retry after 2017-03-13T19:26:54.011Z, rateLimitExceeded

	// Try different resiliency configurations:
	//stream := example_hit_ratelimit(config, tok)
	//stream := example_ratelimited(config, tok)

	// ConcurrentFetchStream and BulkheadStream can be used to increase
	// throughput. The difference is, from caller's perspective, whether
	// concurrency and/or the order of messages need to be manually
	// maintained.
	mp.RegisterMappedStreamMetrics("gmail", "map1")
	mp.RegisterBulkStreamMetrics("gmail", "bulk1")

	go func() {
		for {
			stream := example_ratelimited_retry(config, tok)
			RunWithBulkheadStream(stream, *maxConcurrency, *maxMails)
			//go RunWithConcurrentFetchStream(stream, 300, 2000)
			time.Sleep(time.Minute)
			log.Info("Run again")
		}
	}()
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(":8080", nil)
	log.Crit("Promhttp stoped", "err", err)
}
