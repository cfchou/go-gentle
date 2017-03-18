package main

import (
	"gopkg.in/cfchou/go-gentle.v1/gentle"
	"gopkg.in/inconshreveable/log15.v2"
	"net/http"
	"github.com/hashicorp/go-cleanhttp"
	"bytes"
	"errors"
	"io/ioutil"
	"reflect"
	"math/rand"
	"time"
	"github.com/rs/xid"
	"path/filepath"
	"fmt"
)

var (
	logHandler = log15.MultiHandler(log15.StdoutHandler,
		log15.Must.FileHandler("./test.log", log15.LogfmtFormat()))
	log = log15.New("mixin", "main")
	ErrMessageType = errors.New("Invalid message type")
)

type hesScanReq struct {
	id string
	content []byte
}

func (m *hesScanReq) Id() string {
	return m.id
}

type  hesScanResp struct {
	id string
	content []byte
}

func (m *hesScanResp) Id() string {
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
	s.Log.Debug("Handle() ...", "msg_in", msg.Id())
	var content []byte
	if hmsg, ok := msg.(*hesScanReq); !ok {
		s.Log.Error("Invalid message type", "msg_in", msg.Id(),
			"type", reflect.TypeOf(msg),
			"err", ErrMessageType)
		return nil, ErrMessageType
	} else {
		content = hmsg.content
	}
	resp, err := s.client.Post(s.url, "application/octet-stream",
		bytes.NewReader(content))
	if err != nil {
		s.Log.Error("Post() err", "msg_in", msg.Id(), "err", err)
		return nil, err
	}
	resp_content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		s.Log.Error("ReadAll() err", "msg_in", msg.Id(), "err", err)
		return nil, err
	}
	s.Log.Debug("ReadAll() ok", "msg_in", msg.Id())
	return &hesScanResp{
		id: msg.Id(),
		content:resp_content,
	}, nil
}

type RequestProvider struct {
	Log           log15.Logger
	gen *rand.Rand
	names []string
	requests map[string][]byte
}

func NewRequestProvider(dirname string) *RequestProvider {
	// Reads all files into the memory
	fs, err := ioutil.ReadDir(dirname)
	if err != nil {
		panic(err)
	}
	requests := make(map[string][]byte, len(fs))
	names := []string{}
	for _, f := range fs {
		if f.IsDir() {
			continue
		}
		data, err := ioutil.ReadFile(filepath.Join("tmp", f.Name()))
		if err != nil {
			log.Error("ReadFile err", "err", err)
		} else {
			requests[f.Name()] = data
			names = append(names, f.Name())
		}
	}

	return &RequestProvider{
		Log:       log.New("mixin", "provider"),
		gen: rand.New(rand.NewSource(time.Now().UnixNano())),
		names: names,
		requests: requests,
	}
}

func (r *RequestProvider) Get() (gentle.Message, error) {
	key := r.names[r.gen.Intn(len(r.names))]
	return &hesScanReq{
		id: xid.New().String(),
		content: r.requests[key],
	}, nil
}


func main() {
	provider := NewRequestProvider("tmp")
	msg, _ := provider.Get()
	hmsg := msg.(*hesScanReq)
	fmt.Println(string(hmsg.content))
}
