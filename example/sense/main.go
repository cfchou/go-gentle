package sense

import (
	"gopkg.in/cfchou/go-gentle.v1/gentle"
	"gopkg.in/inconshreveable/log15.v2"
	"net/http"
	"github.com/hashicorp/go-cleanhttp"
	"bytes"
)

var (
	logHandler = log15.MultiHandler(log15.StdoutHandler,
		log15.Must.FileHandler("./test.log", log15.LogfmtFormat()))
	log = log15.New("mixin", "main")
)

type  hesMessage struct {
	id string
	content []byte
}

func (m *hesMessage) Id() string {
	return m.id
}


type HesSend struct {
	Log           log15.Logger
	client *http.Client
	url string
}

func NewHesSend(url string) *HesSend {
	// https://blog.cloudflare.com/the-complete-guide-to-golang-net-http-timeouts/
	return &HesSend{
		Log:       log.New("mixin", "hes_send"),
		client: cleanhttp.DefaultPooledClient(),
		url: url,
	}
}

func (s *HesSend) Handle(msg gentle.Message) (gentle.Message, error) {
	var content []byte
	if hmsg, ok := msg.(hesMessage); !ok {
		panic("Invalid Message Type")
	} else {
		content = hmsg.content
	}
	resp, err := s.client.Post(s.url, "application/octet-stream",
		bytes.NewReader(content))

}