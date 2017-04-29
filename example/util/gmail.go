package util

import (
	"fmt"
	"os"
	"io/ioutil"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/gmail/v1"
	"github.com/cfchou/go-gentle/gentle"
	log15 "gopkg.in/inconshreveable/log15.v2"
	"time"
	"sync"
	"errors"
	"google.golang.org/api/googleapi"
	"strconv"
)

var (
	Log    = log15.New()
	ErrEOF = errors.New("EOF")
	ErrNon2XX = errors.New("Non 2xx returned")

	MIXIN_STREAM_GMAIL_LIST = "list"
	MIXIN_HANDLER_GMAIL_DOWNLOAD = "download"

	// Observation supported by GmailListStream.Get(), it observes the time
	// spent with the labels:
	// "api" = "list"
	// "status" = "err" or "ok"
	MX_STREAM_GMAIL_LIST_GET = "get"

	// Observation supported by GmailMessageHandler.Handle(), it observes
	// the time spent with the labels:
	// "api" = "list"
	// "status" = "err" or "ok"
	MX_HANDLER_GMAIL_HANDLE = "handle"

	// Observation supported by GmailMessageHandler.Handle(), it observes
	// the size of an email downloaded.
	MX_HANDLER_GMAIL_SIZE = "size"
)

// GetTokenFromWeb uses Config to request a Token.
// It returns the retrieved Token.
func GetTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var code string
	if _, err := fmt.Scan(&code); err != nil {
		Log.Error("Unable to read authorization code", "err", err)
		os.Exit(1)
	}

	tok, err := config.Exchange(context.TODO(), code)
	if err != nil {
		Log.Error("Unable to retrieve token from web", "err", err)
		os.Exit(1)
	}
	return tok
}

func GetAppSecret(file string) *oauth2.Config {
	bs, err := ioutil.ReadFile(file)
	if err != nil {
		Log.Error("ReadFile err", "err", err)
		os.Exit(1)
	}

	config, err := google.ConfigFromJSON(bs, gmail.GmailReadonlyScope)
	if err != nil {
		Log.Error("ConfigFromJson err", "err", err)
		os.Exit(1)
	}
	return config
}

func ToGoogleApiErrorCode(err error) string {
	if gerr, ok := err.(*googleapi.Error); ok {
		return strconv.Itoa(gerr.Code)
	}
	return err.Error()
}

type GmailMessage struct {
	Msg *gmail.Message
}

func (m *GmailMessage) Id() string {
	return m.Msg.Id
}

type GmailListStream struct {
	Namespace         string
	Name              string
	Log           log15.Logger
	service       *gmail.Service
	listCall      *gmail.UsersMessagesListCall
	lock          sync.Mutex
	messages      []*gmail.Message
	nextPageToken string
	page_num      int
	page_last     bool
	observation   gentle.Observation
}

func NewGmailListStream(appConfig *oauth2.Config, userTok *oauth2.Token,
	namespace, name string, max_results int64) *GmailListStream {

	client := appConfig.Client(context.Background(), userTok)
	// Timeout for a request
	client.Timeout = time.Second * 30

	service, err := gmail.New(client)
	if err != nil {
		Log.Error("gmail.New err", "err", err)
		os.Exit(1)
	}

	listCall := service.Users.Messages.List("me")
	listCall.MaxResults(max_results)
	return &GmailListStream{
		Namespace: namespace,
		Name:      name,
		Log:       Log.New("mixin", MIXIN_STREAM_GMAIL_LIST),
		service:   service,
		listCall:  listCall,
		lock:      sync.Mutex{},
		page_last: false,
		observation: gentle.NoOpObservationIfNonRegistered(
			&gentle.RegistryKey{namespace,
					    MIXIN_STREAM_GMAIL_LIST,
					    name,
					    MX_STREAM_GMAIL_LIST_GET}),
	}
}

func (s *GmailListStream) nextMessage() (*GmailMessage, error) {
	// assert s.lock is already Locked
	// zero value of a slice is nil
	if s.messages == nil || len(s.messages) == 0 {
		s.Log.Error("Invalid state")
		os.Exit(1)
	}
	msg := &GmailMessage{Msg: s.messages[0]}
	s.messages = s.messages[1:]
	s.Log.Debug("List() nextMessge", "msg", msg.Id(), "page", s.page_num,
		"len_msgs_left", len(s.messages))
	return msg, nil
}

func (s *GmailListStream) Restart(force bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.page_last == false || (s.messages != nil && len(s.messages) > 0) {
		if !force {
			s.Log.Info("Restart ignored, there's still messages")
			return
		}
		s.Log.Info("Restart force to discard messages")
	}
	s.Log.Info("Restart")
	s.page_num = 0
	s.page_last = false
	s.nextPageToken = ""
}

func (s *GmailListStream) Get() (gentle.Message, error) {
	begin := time.Now()
	s.Log.Debug("List() ...")
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.messages != nil && len(s.messages) > 0 {
		msg, _ := s.nextMessage()
		s.Log.Debug("List() ok", "msg", msg.Id(),
			"timespan", time.Now().Sub(begin).Seconds())
		return msg, nil
	}
	// Messages on this page are consumed, fetch next page
	if s.page_last {
		s.Log.Info("List() EOF, no more messages")
		return nil, ErrEOF
	}
	if s.nextPageToken != "" {
		s.listCall.PageToken(s.nextPageToken)
	}
	resp, err := s.listCall.Do()
	timespan := time.Now().Sub(begin).Seconds()
	if err != nil {
		s.observation.Observe(float64(1000 * timespan),
			map[string]string{
				"api": "list",
				"result": "err",
			})
		s.Log.Error("List() err", "err", err, "timespan", timespan)
		return nil, err
	}
	if (resp.HTTPStatusCode < 200 || resp.HTTPStatusCode >= 300) {
		s.observation.Observe(float64(1000 * timespan),
			map[string]string{
				"api": "list",
				"result": "err",
			})
		s.Log.Error("List() not 2xx",
			"status", resp.HTTPStatusCode, "timespan", timespan)
		return nil, ErrNon2XX
	}
	s.observation.Observe(float64(1000 * timespan),
		map[string]string{
			"api": "list",
			"result": "ok",
		})

	if resp.NextPageToken == "" {
		s.Log.Info("List() No more pages","timespan", timespan)
		s.page_last = true
	}

	s.messages = resp.Messages
	s.nextPageToken = resp.NextPageToken
	s.page_num++
	s.Log.Info("List() Read a page", "page", s.page_num,
		"len_msgs", len(s.messages), "nextPageToken", s.nextPageToken,
		"timespan", timespan)
	if s.messages == nil || len(s.messages) == 0 {
		s.Log.Info("List() EOF, no more messages")
		return nil, ErrEOF
	}
	return s.nextMessage()
}


type GmailMessageHandler struct {
	Namespace         string
	Name              string
	Log     log15.Logger
	service *gmail.Service
	observation   gentle.Observation
	totalBytesObservation   gentle.Observation
}

func NewGmailMessageHandler(appConfig *oauth2.Config, userTok *oauth2.Token,
	namespace, name string) *GmailMessageHandler {

	client := appConfig.Client(context.Background(), userTok)
	//client.Timeout = time.Second * 30
	service, err := gmail.New(client)
	if err != nil {
		Log.Error("gmail.New err", "err", err)
		os.Exit(1)
	}
	return &GmailMessageHandler{
		Namespace: namespace,
		Name:      name,
		Log:     Log.New("mixin", MIXIN_HANDLER_GMAIL_DOWNLOAD),
		service: service,
		observation: gentle.NoOpObservationIfNonRegistered(
			&gentle.RegistryKey{namespace,
					    MIXIN_HANDLER_GMAIL_DOWNLOAD,
					    name,
					    MX_HANDLER_GMAIL_HANDLE}),
		totalBytesObservation: gentle.NoOpObservationIfNonRegistered(
			&gentle.RegistryKey{namespace,
					    MIXIN_HANDLER_GMAIL_DOWNLOAD,
					    name,
					    MX_HANDLER_GMAIL_SIZE}),
	}
}

func (h *GmailMessageHandler) Handle(msg gentle.Message) (gentle.Message, error) {
	h.Log.Debug("Message.Get() ...", "msg_in", msg.Id())
	getCall := h.service.Users.Messages.Get("me", msg.Id())
	getCall.Format("raw")
	callStart := time.Now()
	gmsg, err := getCall.Do()
	timespan := time.Now().Sub(callStart).Seconds()
	if err != nil {
		h.observation.Observe(float64(1000 * timespan),
			map[string]string{
				"api": "get",
				"result": "err",
			})
		h.Log.Error("Messages.Get() err", "msg_in", msg.Id(),
			"err", err, "timespan", timespan)
		return nil, err
	}
	if (gmsg.HTTPStatusCode < 200 || gmsg.HTTPStatusCode >= 300) {
		h.observation.Observe(float64(1000 * timespan),
			map[string]string{
				"api": "get",
				"result": "err",
			})
		h.Log.Error("List() not 2xx",
			"status", gmsg.HTTPStatusCode, "timespan", timespan)
		return nil, ErrNon2XX
	}
	h.observation.Observe(float64(1000 * timespan),
		map[string]string{
			"api": "get",
			"result": "ok",
		})
	h.totalBytesObservation.Observe(float64(gmsg.SizeEstimate),
		map[string]string{})
	h.Log.Debug("Messages.Get() ok", "msg_in", msg.Id(),
		"msg_out", gmsg.Id, "size", gmsg.SizeEstimate,
		"timespan", timespan)
	return &GmailMessage{Msg: gmsg}, nil
}



