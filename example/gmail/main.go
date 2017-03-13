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

type gmailStream struct {
	service       *gmail.Service
	listCall      *gmail.UsersMessagesListCall
	lock          sync.Mutex
	messages      []*gmail.Message
	nextPageToken string
	page_num      int
}

func (s *gmailStream) nextMessage() (*gmailMessage, error) {
	// assert s.lock is Locked
	if s.messages == nil || len(s.messages) == 0 {
		log.Error("Invalid state")
		os.Exit(1)
	}
	msg := &gmailMessage{msg: s.messages[0]}
	s.messages = s.messages[1:]
	return msg, nil
}

func NewGmailStream(config *oauth2.Config) *gmailStream {
	tok := getTokenFromWeb(config)
	client := config.Client(context.Background(), tok)
	// Timeout for a request
	client.Timeout = time.Second * 30

	service, err := gmail.New(client)
	if err != nil {
		log.Error("gmail.New err", "err", err)
		os.Exit(1)
	}

	listCall := service.Users.Messages.List("me")
	return &gmailStream{
		service:  service,
		listCall: listCall,
		lock:     sync.Mutex{},
	}
}

func (s *gmailStream) Get() (gentle.Message, error) {
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
		log.Info("EOF, no more pages")
		return nil, ErrEOF
	}
	s.messages = resp.Messages
	s.nextPageToken = resp.NextPageToken
	log.Info("Read a page", "page", s.page_num+1,
		"len_msgs", len(s.messages), "nextPageToken", s.nextPageToken)
	if len(s.messages) == 0 {
		log.Info("EOF, no more messages")
		return nil, ErrEOF
	}
	s.page_num++
	return s.nextMessage()
}

func main() {
	h := log15.LvlFilterHandler(log15.LvlInfo, log15.StdoutHandler)
	log.SetHandler(h)

	count := 2000
	total := 0
	config := getAppSecret(app_secret_file)
	stream := NewGmailStream(config)
	for i := 0; i < count; i++ {
		msg, err := stream.Get()
		if err != nil {
			log.Error("Get() err", "err", err)
			break
		}
		total++
		log.Debug("Got message", "msg", msg.Id())
	}
	fmt.Printf("total messages: %d\n", total)
}
