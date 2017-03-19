package main

import (
	"bufio"
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/hashicorp/go-cleanhttp"
	"github.com/pborman/uuid"
	"github.com/rs/xid"
	"github.com/spf13/pflag"
	"gopkg.in/cfchou/go-gentle.v1/gentle"
	"gopkg.in/inconshreveable/log15.v2"
	"io/ioutil"
	"math/rand"
	"net/http"
	"path/filepath"
	"reflect"
	"time"
	"sync/atomic"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const max_int64 = int64(^uint64(0) >> 1)

var (
	log            = log15.New("mixin", "main")
	ErrMessageType = errors.New("Invalid message type")

	// command line options
	url       = pflag.String("url", "http://127.0.0.1:8080", "HES url")
	dir       = pflag.String("dir", "mails", "directory contains mails")
	isDryRun  = pflag.BoolP("dryrun", "d", false, "dry run doesn't send reqests")
	max_concurrency = pflag.Int("max-concurrency", 1, "max concurrent requests")
	max_mails = pflag.Int64("max-mails", max_int64, "max number of emails")
)

func init() {
	pflag.Parse()
}

type hesScanReq struct {
	id      string
	content []byte
}

func (m *hesScanReq) Id() string {
	return m.id
}

type hesScanResp struct {
	id      string
	status  string
	content []byte
}

func (m *hesScanResp) Id() string {
	return m.id
}

type HesSend struct {
	Log    log15.Logger
	client *http.Client
	url    string
}

func NewHesSend(url string) *HesSend {
	// https://blog.cloudflare.com/the-complete-guide-to-golang-net-http-timeouts/
	return &HesSend{
		Log:    log.New("mixin", "hes_send"),
		client: cleanhttp.DefaultPooledClient(),
		url:    url,
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
	//fmt.Println(resp.Status)
	body_content, err := ioutil.ReadAll(resp.Body)
	//fmt.Println(string(body_content))
	if err != nil {
		s.Log.Error("ReadAll() err", "msg_in", msg.Id(), "err", err)
		return nil, err
	}
	//s.Log.Debug("ReadAll() ok", "msg_in", msg.Id())
	s.Log.Debug("ReadAll() ok", "msg_in", msg.Id(),
		"resp", string(body_content))
	return &hesScanResp{
		id:      msg.Id(),
		status: resp.Status,
		content: body_content,
	}, nil
}

type RequestProvider struct {
	Log      log15.Logger
	gen      *rand.Rand
	names    []string
	requests map[string][]byte
	userData *UserData
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
		data, err := ioutil.ReadFile(filepath.Join(dirname, f.Name()))
		if err != nil {
			log.Error("ReadFile err", "err", err)
		} else {
			requests[f.Name()] = data
			names = append(names, f.Name())
		}
	}

	return &RequestProvider{
		Log:      log.New("mixin", "provider"),
		gen:      rand.New(rand.NewSource(time.Now().UnixNano())),
		names:    names,
		requests: requests,
		userData: &UserData{
			Policy: &Policy{
				PolicyDomain: "default-domain.hes.trendmicro.com",
				PolicyRules: []string{
					"d3515bf2-c1ce-4c60-aed8-ff0fe88078fb",
					"38757cfd-8de7-41b9-a75b-c663e7ca234b",
					"57f35898-6e42-4a0d-bba3-6180ee960af7",
					"393c3e59-0e06-4ea4-893e-f91db88c3e2b",
				},
			},
		},
	}
}

/*
<meta-data>
	<task-id-chain>
		<task-id>aca4058e-2000-4194-a801-067922ec4968</task-id>
	</task-id-chain>
	<user-data>
		<policy>
			<policy_domain>default-domain.hes.trendmicro.com</policy_domain>
			<policy_rules>
				<policy_rule>d3515bf2-c1ce-4c60-aed8-ff0fe88078fb</policy_rule>
				<policy_rule>38757cfd-8de7-41b9-a75b-c663e7ca234b</policy_rule>
				<policy_rule>57f35898-6e42-4a0d-bba3-6180ee960af7</policy_rule>
				<policy_rule>393c3e59-0e06-4ea4-893e-f91db88c3e2b</policy_rule>
			</policy_rules>
		</policy>
	</user-data>
</meta-data>
*/

type TaskIdChain struct {
	XMLName xml.Name `xml:"task-id-chain"`
	TaskId  []string `xml:"task-id"`
}

type UserData struct {
	XMLName xml.Name `xml:"user-data"`
	Policy  *Policy
}

type Policy struct {
	XMLName      xml.Name `xml:"policy"`
	PolicyDomain string   `xml:"policy_domain"`
	PolicyRules  []string `xml:"policy_rules>policy_rule"`
}

type metaData struct {
	XMLName     xml.Name `xml:"meta-data"`
	TaskIdChain *TaskIdChain
	UserData    *UserData
}

func (r *RequestProvider) addMeta(id string, content []byte) []byte {
	var meta_buf bytes.Buffer
	meta_writer := bufio.NewWriter(&meta_buf)

	//enc := xml.NewEncoder(os.Stdout)
	//enc := xml.NewEncoder(meta_buf)
	enc := xml.NewEncoder(meta_writer)
	data := &metaData{
		TaskIdChain: &TaskIdChain{
			TaskId: []string{uuid.New()},
		},
		UserData: r.userData,
	}
	meta_writer.WriteString(`<?xml version="1.0"?>`)
	enc.Encode(data)
	// Encode calls Flush before returning.
	// Writer.Flush to calculate current len of meta_buf, which is going to
	// be embedded in payload.
	meta_writer.Flush()

	payload := fmt.Sprintf("%08x%s00000002META%08x",
		len(data.TaskIdChain.TaskId[0]),
		data.TaskIdChain.TaskId[0], meta_buf.Len())

	meta_writer.WriteString(fmt.Sprintf("TASK%08x", len(content)))
	meta_writer.Write(content)
	meta_writer.WriteString("FFFFFFFF")
	meta_writer.Flush()

	meta_buf.Bytes()
	all_bytes := []byte(payload)
	all_bytes = append(all_bytes, meta_buf.Bytes()[:]...)
	// correlate msg.Id & task id in log
	r.Log.Debug("addMeta ok", "msg", id, "task", data.TaskIdChain.TaskId[0],
		"all_len", len(all_bytes))
	return all_bytes
}

func (r *RequestProvider) Get() (gentle.Message, error) {
	id := xid.New().String()
	key := r.names[r.gen.Intn(len(r.names))]
	return &hesScanReq{
		id:      id,
		content: r.addMeta(id, r.requests[key]),
	}, nil
}

func runOne() {
	provider := NewRequestProvider(*dir)
	msg, _ := provider.Get()
	hreq := msg.(*hesScanReq)
	if *isDryRun {
		fmt.Println("Is a dry run. Exit")
		return
	}

	hes := NewHesSend(*url + "/scanner/mail")
	hmsg, err := hes.Handle(hreq)
	if err != nil {
		log.Error("Handle err", "err", err)
	}
	hresp := hmsg.(*hesScanResp)
	fmt.Println(string(hresp.content))
}

type timedResult struct {
	msg gentle.Message
	dura time.Duration
}

func runStream(stream gentle.Stream, concurrent_num int, count int64) {
	var total int64
	var success_total int64

	result := make(chan *struct{}, concurrent_num)
	for i := int64(0); i < count; i++ {
		result <- &struct {}{}
		go func() {
			//begin := time.Now()
			msg, err := stream.Get()
			atomic.AddInt64(&total, 1)
			if err != nil {
				log.Error("Get() err", "err", err)
			} else {
				log.Debug("Get() ok", "msg", msg.Id())
				atomic.AddInt64(&success_total, 1)
			}
			<-result
		}()
	}
}

func main() {
	h := log15.MultiHandler(log15.StdoutHandler,
		log15.Must.FileHandler("./test.log", log15.LogfmtFormat()))
	log.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, h))

	gentle.Log.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, h))

	provider := NewRequestProvider(*dir)
	hes := NewHesSend(*url + "/scanner/mail")
	stream := gentle.NewMappedStream("sense", provider, hes)
	go runStream(stream, *max_concurrency, *max_mails)
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(":8080", nil)
	log.Crit("Promhttp stoped", "err", err)
}
