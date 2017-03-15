package main

import (
	"fmt"
	"io/ioutil"

	"errors"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/gmail/v1"
	"gopkg.in/cfchou/go-gentle.v1/gentle"
	log15 "gopkg.in/inconshreveable/log15.v2"
	"os"
	"sync"
	"time"
)

const (
	app_secret_file = "app_secret.json"
)

var logHandler = log15.MultiHandler(log15.StdoutHandler,
	log15.Must.FileHandler("./test.log", log15.LogfmtFormat()))
var log = log15.New("mixin", "main")
var ErrEOF = errors.New("EOF")

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
		service:   service,
		listCall:  listCall,
		lock:      sync.Mutex{},
		Log:       log.New("mixin", "list"),
		page_last: false,
		terminate: make(chan *struct{}),
	}
}

func (s *gmailListStream) nextMessage() (*gmailMessage, error) {
	// assert s.lock is Locked
	if s.messages == nil || len(s.messages) == 0 {
		s.Log.Error("Invalid state")
		os.Exit(1)
	}
	msg := &gmailMessage{msg: s.messages[0]}
	s.messages = s.messages[1:]
	return msg, nil
}

func (s *gmailListStream) shutdown() {
	s.terminate <- &struct{}{}
}

func (s *gmailListStream) Get() (gentle.Message, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.messages != nil && len(s.messages) > 0 {
		return s.nextMessage()
	}
	// Messages on this page are consumed, fetch next page
	if s.page_last {
		s.Log.Info("EOF, no more messages and pages")
		select {
		case <-s.terminate:
			return nil, ErrEOF
		}
	}
	if s.nextPageToken != "" {
		s.listCall.PageToken(s.nextPageToken)
	}
	resp, err := s.listCall.Do()
	if err != nil {
		s.Log.Error("List() err", "err", err)
		return nil, err
	}

	if resp.NextPageToken == "" {
		s.Log.Info("No more pages")
		s.page_last = true
	}

	s.messages = resp.Messages
	s.nextPageToken = resp.NextPageToken
	s.page_num++
	s.Log.Info("Read a page", "page", s.page_num,
		"len_msgs", len(s.messages), "nextPageToken", s.nextPageToken)
	if len(s.messages) == 0 {
		s.Log.Info("EOF, no more messages")
		select {
		case <-s.terminate:
			return nil, ErrEOF
		}
	}
	return s.nextMessage()
}

type gmailMessageHandler struct {
	service *gmail.Service
	Log     log15.Logger
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
		service: service,
		Log:     log.New("mixin", "download"),
	}
}

func (h *gmailMessageHandler) Handle(msg gentle.Message) (gentle.Message, error) {
	getCall := h.service.Users.Messages.Get("me", msg.Id())
	gmsg, err := getCall.Do()
	if err != nil {
		h.Log.Error("Messages.Get() err", "err", err)
		return nil, err
	}
	h.Log.Debug("Messages.Get() ok", "size", gmsg.SizeEstimate)
	return &gmailMessage{msg: gmsg}, nil
}

func example_list_only(appConfig *oauth2.Config, userTok *oauth2.Token) gentle.Stream {
	lstream := NewGmailListStream(appConfig, userTok, 500)
	return lstream
}

func example_hit_ratelimit(appConfig *oauth2.Config, userTok *oauth2.Token) gentle.Stream {

	lstream := NewGmailListStream(appConfig, userTok, 500)
	lstream.Log.SetHandler(log15.LvlFilterHandler(log15.LvlInfo, logHandler))

	mstream := gentle.NewMappedStream("gmail", lstream,
		NewGmailMessageHandler(appConfig, userTok))

	// This is likely to hit error:
	// googleapi: Error 429: Too many concurrent requests for user, rateLimitExceeded
	// googleapi: Error 429: User-rate limit exceeded.  Retry after 2017-03-13T19:26:54.011Z, rateLimitExceeded
	return gentle.NewConcurrentFetchStream("gmail", mstream, 300)
}

func example_ratelimited(appConfig *oauth2.Config, userTok *oauth2.Token) gentle.Stream {

	lstream := NewGmailListStream(appConfig, userTok, 500)
	lstream.Log.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, logHandler))

	handler := NewGmailMessageHandler(appConfig, userTok)

	rhandler := gentle.NewRateLimitedHandler("gmail", handler,
		// (1000/request_interval) messages/sec, but it's an upper
		// bound, the real speed is likely much lower.
		gentle.NewTokenBucketRateLimit(1, 1))

	mstream := gentle.NewMappedStream("gmail", lstream, rhandler)
	mstream.Log.SetHandler(logHandler)

	return gentle.NewConcurrentFetchStream("gmail", mstream, 300)
}

func example_ratelimited_retry(appConfig *oauth2.Config, userTok *oauth2.Token) gentle.Stream {

	lstream := NewGmailListStream(appConfig, userTok, 500)
	lstream.Log.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, logHandler))

	handler := NewGmailMessageHandler(appConfig, userTok)

	rhandler := gentle.NewRateLimitedHandler("gmail", handler,
		// (1000/request_interval) messages/sec, but it's an upper
		// bound, the real speed is likely much lower.
		gentle.NewTokenBucketRateLimit(1, 1))

	rthandler := gentle.NewRetryHandler("gmail", rhandler, []time.Duration{
		20 * time.Millisecond, 40 * time.Millisecond, 80 * time.Millisecond})
	rthandler.Log.SetHandler(logHandler)

	mstream := gentle.NewMappedStream("gmail", lstream, rthandler)
	mstream.Log.SetHandler(logHandler)

	return gentle.NewConcurrentFetchStream("gmail", mstream, 300)
}

func main() {
	h := log15.LvlFilterHandler(log15.LvlDebug, logHandler)
	log.SetHandler(h)
	config := getAppSecret(app_secret_file)
	tok := getTokenFromWeb(config)

	count := 2000
	// total should be, if gmail Messages.List() doesn't return error, the
	// total of all gmailListStream emits pluses 1(ErrEOF).
	total := 0
	// success_total should be, the number of mails have been successfully
	// downloaded.
	success_total := 0
	var totalSize int64
	//stream := example_hit_ratelimit(config, tok)
	//stream := example_ratelimited(config, tok)
	stream := example_ratelimited_retry(config, tok)

	total_begin := time.Now()
	var total_time_success time.Duration
	mails := make(map[string]bool)
	for i := 0; i < count; i++ {
		total++
		begin := time.Now()
		msg, err := GetWithTimeout(stream, 10*time.Second)
		dura := time.Now().Sub(begin)
		if err != nil {
			if err == ErrEOF {
				log.Error("Got() EOF")
				break
			} else {
				log.Error("Got() err", "err", err)
				continue
			}
		}
		gmsg := msg.(*gmailMessage).msg
		log.Debug("Got message", "msg", gmsg.Id,
			"size", gmsg.SizeEstimate, "dura", dura)
		total_time_success += dura
		success_total++
		totalSize += gmsg.SizeEstimate
		// Test duplication
		if _, existed := mails[msg.Id()]; existed {
			log.Error("Duplicated Messagge", "msg", gmsg.Id)
			break
		}
		mails[msg.Id()] = true
	}
	log.Info("Done", "total", total, "success_total", success_total,
		"total_size", totalSize,
		"total_time", time.Now().Sub(total_begin),
		"success_time", total_time_success)
	fmt.Printf("total: %d, success_total: %d, size: %d, "+
		"total_time: %s, success_time: %s\n",
		total, success_total, totalSize,
		time.Now().Sub(total_begin), total_time_success)
}

func GetWithTimeout(stream gentle.Stream, timeout time.Duration) (gentle.Message, error) {
	tm := time.NewTimer(timeout)
	result := make(chan interface{})
	go func() {
		msg, err := stream.Get()
		if err != nil {
			log.Error("stream.Get() err", "err", err)
			result <- err
		} else {
			result <- msg
		}
	}()
	var v interface{}
	select {
	case v = <-result:
	case <-tm.C:
		log.Error("timeout expired")
		return nil, ErrEOF
	}
	if inst, ok := v.(gentle.Message); ok {
		return inst, nil
	} else {
		return nil, v.(error)
	}
}
