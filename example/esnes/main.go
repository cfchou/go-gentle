package main

import (
	"gopkg.in/redis.v5"
	"gopkg.in/inconshreveable/log15.v2"
	"net/http"
	"time"
	"strings"
	"gopkg.in/cfchou/go-gentle.v1/gentle"
	"github.com/hashicorp/go-cleanhttp"
	"github.com/spf13/pflag"
	"fmt"
	"io/ioutil"
	"errors"
	"strconv"
	"sync/atomic"
)

const max_int64 = int64(^uint64(0) >> 1)

var (
	log            = log15.New("mixin", "main")
	ErrParse	= errors.New("Parse error")
	ErrNoContent	= errors.New("No content")

	// command line options
	url             = pflag.String("url", "http://127.0.0.1:8080", "HES url")
	db              = pflag.Int("redis-db", 0, "the db used in redis")
	pop_count = pflag.Int("pop-count", 51, "HES pop count")
	pop_size = pflag.Int64("pop-size", 16777216, "HES pop size")

	max_concurrency = pflag.Int("max-concurrency", 1, "max concurrent requests")
	max_recvs = pflag.Int64("max-recvs", max_int64, "max recv requests to HES")
	max_recvs_sec	= pflag.Int("max-recvs-sec", 10, "rate limit of max recv per second")
)

func init() {
	pflag.Parse()
}

type HesRecvStream struct {
	Log    log15.Logger
	client *http.Client
	url    string
	rd	*redis.Client
	body	string
}


const recv_format =
`<p1:common_pop_request xmlns:p1="http://www.trendmicro.com/nebula/xml_schema">
<pop_count>%d</pop_count>
<pop_size>%d</pop_size>
<dest_host>10.64.70.20</dest_host>
</p1:common_pop_request>`

func NewHesRecvStream(url string, rd *redis.Client) *HesRecvStream {
	return &HesRecvStream{
		Log:    log.New("mixin", "hes_send"),
		client: cleanhttp.DefaultPooledClient(),
		url:    url,
		rd:     rd,
		body:   fmt.Sprintf(recv_format, *pop_count, *pop_size),
	}
}

func (s *HesRecvStream) Get() (gentle.Message, error) {

	req, err := http.NewRequest(http.MethodPut, s.url, strings.NewReader(s.body))
	if err != nil {
		s.Log.Error("NewRequest err", "err", err)
		return nil, err
	}
	// use redis' server time as recv_req_begin
	rdtime, err := s.rd.Time().Result()
	if err != nil {
		return nil, err
	}

	begin := time.Now()
	resp, err := s.client.Do(req)
	if err != nil {
		s.Log.Error("PUT err", "err", err)
		return nil, err
	}
	if resp.StatusCode == http.StatusNoContent {
		s.Log.Debug("PUT ok, but no content")
		return nil, ErrNoContent
	}

	timespan := time.Now().Sub(begin)
	s.Log.Debug("PUT timespan",
		"begin", begin.Format(time.StampMilli),
		"timespan", timespan)
	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		s.Log.Error("ReadAll() err", "err", err)
		return nil, err
	}
	s.Log.Debug("ReadAll() ok", "body_len", len(content))
	body := string(content)
	meta, ok := parse_hes_info("meta-data", body)
	if !ok {
		// This could be that it's not 200
		s.Log.Error("Parse meta err")
		return nil, ErrParse
	}
	s.Log.Debug("parse meta ok", "meta_len", len(meta))
	taskId, ok := parse_hes_info("task-id", meta)
	if !ok {
		s.Log.Error("Parse taskId err")
		return nil, ErrParse
	}

	// Update a hash on redis; key is taskId
	// recv_resp_len: 	int, resp content-length in bytes
	recvData := map[string]string{
		"recv_req_begin": strconv.FormatInt(rdtime.Unix(), 10),
		"recv_req_dura": strconv.FormatFloat(timespan.Seconds(), 'f',
			3, 64),
		"recv_resp_len": strconv.FormatInt(resp.ContentLength, 10),
	}
	//go func() {
		_, err = s.rd.HMSet(taskId, recvData).Result()
		if err != nil {
			s.Log.Debug("HMSet err", "msg", taskId, "err", err)
		} else {
			s.Log.Debug("HMSet ok", "msg", taskId)
		}
	//}()
	return &hesRecvResp{
		id: taskId,
	}, nil
}

type hesRecvResp struct {
	id      string
}

func (m *hesRecvResp) Id() string {
	return m.id
}

func parse_hes_info(key string, body string) (string, bool) {
	// A legitimate response is like(line-breaks added for readibility):
	// 00000024a05b96c7-7446-4d5a-8ecd-b8f55216fc4100000002META00000e3f<meta-data>....
	// <task-id>a05b96c7-7446-4d5a-8ecd-b8f55216fc41</task-id>
	// ...</meta-data>
	key_begin := "<" + key + ">"
	key_end := "</" + key + ">"
	i := strings.Index(body, key_begin)
	si := i + len(key_begin)
	sj := strings.Index(body, key_end)
	if i == -1 || si >= sj {
		return "", false
	}
	return body[si:sj], true
}

func runLoop() {
	rd := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       *db,  // use default DB
	})
	var stream gentle.Stream
	interval := 1000 / (*max_recvs_sec)
	if interval > 0 {
		log.Debug("Rate limit enabled, pause in millis between every scan",
			"pause", interval)
		stream = gentle.NewRateLimitedStream("esnes",
			NewHesRecvStream(*url + "/scanner/deliver", rd),
			gentle.NewTokenBucketRateLimit(interval, 1))
	} else {
		stream = NewHesRecvStream(*url + "/scanner/deliver", rd)
	}
	runStream(stream, *max_concurrency, *max_recvs)
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
		log15.Must.FileHandler("./esnes.log", log15.LogfmtFormat()))
	log.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, h))
	gentle.Log.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, h))

	runLoop()

	// block until keyboard interrupt
	var block <-chan *struct{}
	<-block
}
