package main

import (
	"fmt"
	"io/ioutil"

	"errors"
	"github.com/cfchou/go-gentle/gentle"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/gmail/v1"
	log15 "gopkg.in/inconshreveable/log15.v2"
	"os"
	"sync"
	"time"
)

const (
	app_secret_file = "app_secret.json"
)

var log = log15.New()
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
	Log log15.Logger
	service       *gmail.Service
	listCall      *gmail.UsersMessagesListCall
	lock          sync.Mutex
	messages      []*gmail.Message
	nextPageToken string
	page_num      int
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
		service:  service,
		listCall: listCall,
		lock:     sync.Mutex{},
		Log:     log.New("mixin", "list"),
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

func (s *gmailListStream) Get() (gentle.Message, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.messages != nil && len(s.messages) > 0 {
		return s.nextMessage()
	}
	if s.nextPageToken != "" {
		s.listCall.PageToken(s.nextPageToken)
	}
	resp, err := s.listCall.Do()
	if err != nil {
		return nil, err
	}
	if resp.NextPageToken == "" && s.nextPageToken == "" {
		s.Log.Info("EOF, no more pages")
		return nil, ErrEOF
	}
	s.messages = resp.Messages
	s.nextPageToken = resp.NextPageToken
	s.Log.Info("Read a page", "page", s.page_num+1,
		"len_msgs", len(s.messages), "nextPageToken", s.nextPageToken)
	if len(s.messages) == 0 {
		s.Log.Info("EOF, no more messages")
		return nil, ErrEOF
	}
	s.page_num++
	return s.nextMessage()
}

type gmailMessageHandler struct {
	service *gmail.Service
	Log log15.Logger
}

func NewGmailMessageHandler(appConfig *oauth2.Config, userTok *oauth2.Token) *gmailMessageHandler {
	client := appConfig.Client(context.Background(), userTok)
	// Timeout for a request
	client.Timeout = time.Second * 30
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
	return &gmailMessage{ msg:gmsg }, nil
}

func example_list_only(appConfig *oauth2.Config, userTok *oauth2.Token) gentle.Stream {
	lstream := NewGmailListStream(appConfig, userTok, 500)
	lstream.Log.SetHandler(log15.LvlFilterHandler(log15.LvlInfo, log15.StdoutHandler))
	return lstream
}

func example_happy_slow(appConfig *oauth2.Config, userTok *oauth2.Token) gentle.Stream {
	lstream := NewGmailListStream(appConfig, userTok, 500)
	lstream.Log.SetHandler(log15.LvlFilterHandler(log15.LvlInfo, log15.StdoutHandler))

	mstream := gentle.NewMappedStream("gmail", lstream,
		NewGmailMessageHandler(appConfig, userTok))

	// Gmail per-user throttle is capped at 250 msg/s
	return gentle.NewConcurrentFetchStream("gmail", mstream, 100)
}

func example_hit_ratelimit(appConfig *oauth2.Config, userTok *oauth2.Token) gentle.Stream {

	lstream := NewGmailListStream(appConfig, userTok, 500)
	lstream.Log.SetHandler(log15.LvlFilterHandler(log15.LvlInfo, log15.StdoutHandler))

	mstream := gentle.NewMappedStream("gmail", lstream,
		NewGmailMessageHandler(appConfig, userTok))

	// Gmail per-user throttle is capped at 250 msg/s
	// googleapi: Error 429: Too many concurrent requests for user, rateLimitExceeded
	return gentle.NewConcurrentFetchStream("gmail", mstream, 300)
}

func example_ratelimited(appConfig *oauth2.Config, userTok *oauth2.Token) gentle.Stream {

	lstream := NewGmailListStream(appConfig, userTok, 500)
	lstream.Log.SetHandler(log15.LvlFilterHandler(log15.LvlInfo, log15.StdoutHandler))

	handler := NewGmailMessageHandler(appConfig, userTok)

	// 4
	rhandler := gentle.NewRateLimitedHandler("gmail", handler,
		gentle.NewTokenBucketRateLimit(2, 1))

	mstream := gentle.NewMappedStream("gmail", lstream, rhandler)

	return gentle.NewConcurrentFetchStream("gmail", mstream, 300)
}

func main() {
	h := log15.LvlFilterHandler(log15.LvlDebug, log15.StdoutHandler)
	log.SetHandler(h)
	config := getAppSecret(app_secret_file)
	tok := getTokenFromWeb(config)

	count := 2000
	total := 0
	//stream := example_hit_ratelimit(config, tok)
	//stream := example_ratelimited(config, tok)
	stream := example_happy_slow(config, tok)

	begin := time.Now()
	mails := make(map[string]bool)
	for i := 0; i < count; i++ {
		msg, err := stream.Get()
		if err != nil {
			if err == ErrEOF {
				log.Error("Get() EOF")
			} else {
				log.Error("Get() err", "err", err)
			}
			break
		}
		total++
		log.Debug("Got message", "msg", msg.Id())
		// Test duplication
		if _, existed := mails[msg.Id()]; existed {
			log.Error("Duplicated Messagge", "msg", msg.Id())
			break
		}
		mails[msg.Id()] = true
	}
	fmt.Printf("total messages: %d, take: %s\n", total, time.Now().Sub(begin))
}
