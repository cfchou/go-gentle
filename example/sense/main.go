package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/hashicorp/go-cleanhttp"
	"github.com/rs/xid"
	"github.com/spf13/pflag"
	"gopkg.in/cfchou/go-gentle.v1/gentle"
	"gopkg.in/inconshreveable/log15.v2"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const max_int64 = int64(^uint64(0) >> 1)

var (
	log            = log15.New("mixin", "main")
	ErrMessageType = errors.New("Invalid message type")

	// command line options
	url            = pflag.String("url", "http://localhost:8080", "HES url")
	dir            = pflag.String("dir", "mails", "directory contains mails")
	csvFile        = pflag.String("csv-file", "sense.csv", "csv filename")
	csvBufRows     = pflag.Int("csv-buf-rows", 2048, "max buffered rows in csv befor write")
	csvFlush       = pflag.Int("csv-flush", 3, "csv flush interval in secs")
	logFile        = pflag.String("log-file", "sense.log", "log filename")
	maxConcurrency = pflag.Int("max-concurrency", 1, "max concurrent requests")
	maxScans       = pflag.Int64("max-scans", 100, "max scan requests to HES")
	maxScansSec    = pflag.Int("max-scans-sec", 10, "rate limit of max scans per second")
	isDryRun       = pflag.BoolP("dryrun", "d", false, "dry run doesn't send requests")
)

func init() {
	pflag.Parse()
}

type hesScanReq struct {
	id      string
	mailId  string
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

type HesSendHandler struct {
	Log     log15.Logger
	client  *http.Client
	url     string
	csvChan chan []string
}

func NewHesSendHandler(url string, csvChan chan []string) *HesSendHandler {
	// https://blog.cloudflare.com/the-complete-guide-to-golang-net-http-timeouts/
	return &HesSendHandler{
		Log:     log.New("mixin", "hes_send"),
		client:  cleanhttp.DefaultPooledClient(),
		url:     url,
		csvChan: csvChan,
	}
}

func (s *HesSendHandler) Handle(msg gentle.Message) (gentle.Message, error) {
	s.Log.Debug("Handle() ...", "msg_in", msg.Id())
	hmsg, ok := msg.(*hesScanReq)
	if !ok {
		s.Log.Error("Invalid message type", "msg_in", msg.Id(),
			"type", reflect.TypeOf(msg),
			"err", ErrMessageType)
		return nil, ErrMessageType
	}
	/*
		Create a row in csv:

		task_id: msg.id()
		mail_id: "the email's unique file name"
		mail_sz: 12345			// int, mail size in bytes
		scan_req_begin: 1490064094,	// int, unix timestamp in secs

		// Following are not guaranteed to be available.
		scan_req_dura: 1.001, 		// float, in secs
		scan_req_len: 1024		// int, req content-length in bytes
		scan_resp_status: "200"		// string, HTTP response status
		scan_resp_hes_state: "2"	// string, HES info
		scan_resp_hes_in: "1"		// string, HES info
		scan_resp_hes_out: "0"		// string, HES info
		scan_resp_hes_doing: "0"	// string, HES info

		columns := []string{"task_id", "mail_id", "mail_sz",
			"scan_req_begin", "scan_req_dura", "scan_req_len",
			"scan_resp_status", "scan_resp_hes_state",
			"scan_resp_hes_in", "scan_resp_hes_out", "scan_resp_hes_doing"}
	*/

	begin := time.Now()
	// "task_id", "mail_id", "mail_sz", "scan_req_begin"
	// row := []string{hmsg.id, hmsg.mailId, strconv.Itoa(len(hmsg.content)),
	// 	strconv.FormatInt(begin.Unix(), 10)}
	row := []string{hmsg.id, hmsg.mailId, strconv.Itoa(len(hmsg.content)),
			strconv.FormatFloat(
				float64(begin.UnixNano()/int64(time.Millisecond))/1000,
				'f', 3, 64)}

	// defer set here, only "task_id", "mail_id", "mail_sz" and
	// "scan_req_begin" are guaranteed to exist
	defer func() {
		s.csvChan <- row
	}()

	resp, err := s.client.Post(s.url, "application/octet-stream",
		bytes.NewReader(hmsg.content))
	if err != nil {
		s.Log.Error("POST err", "msg_in", msg.Id(), "err", err)
		return nil, err
	}
	timespan := time.Now().Sub(begin)

	// "scan_req_dura", "scan_req_len"
	row = append(row, strconv.FormatFloat(timespan.Seconds(), 'f', 3, 64),
		strconv.FormatInt(resp.Request.ContentLength, 10))

	s.Log.Debug("POST timespan", "msg_in", msg.Id(),
		"begin", begin.Format(time.StampMilli), "timespan", timespan)

	// "scan_resp_status"
	row = append(row, resp.Status)

	if resp.StatusCode != 200 {
		s.Log.Warn("POST returns suspicious status",
			"status", resp.Status)
		return nil, errors.New(resp.Status)
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		s.Log.Error("ReadAll() err", "msg_in", msg.Id(), "err", err)
		return nil, err
	}
	var hes_state, hes_in, hes_out, hes_doing string
	body := string(content)
	if hes_state, ok = parse_hes_info("state", body); ok {
		// "scan_resp_hes_state"
		row = append(row, hes_state)
	}
	if hes_in, ok = parse_hes_info("in_queue_size", body); ok {
		// "scan_resp_hes_in"
		row = append(row, hes_in)
	}
	if hes_out, ok = parse_hes_info("out_queue_size", body); ok {
		// "scan_resp_hes_out"
		row = append(row, hes_out)
	}
	if hes_doing, ok = parse_hes_info("doing_count", body); ok {
		row = append(row, hes_doing)
	}
	s.Log.Debug("ReadAll() ok", "msg_in", msg.Id(),
		"status", resp.Status, "hes_state", hes_state,
		"hes_in", hes_in, "hes_out", hes_out, "hes_doing", hes_doing)

	return &hesScanResp{
		id:      msg.Id(),
		status:  resp.Status,
		content: content,
	}, nil
}

func parse_hes_info(key string, body string) (string, bool) {
	// A legitimate response is like(line-breaks added for readibility):
	// <p1:common_push_response xmlns:p1="http://www.trendmicro.com/nebula/xml_schema">
	// <state>2</state>
	// <in_queue_size>1</in_queue_size>
	// <out_queue_size>0</out_queue_size><doing_count>0</doing_count>
	// </p1:common_push_response>
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

type RequestStream struct {
	Log      log15.Logger
	gen      *rand.Rand
	names    []string
	requests map[string][]byte
	userData *UserData
}

func NewRequestStream(dirname string) *RequestStream {
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

	return &RequestStream{
		Log:      log.New("mixin", "request"),
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

type MetaData struct {
	XMLName     xml.Name `xml:"meta-data"`
	TaskIdChain *TaskIdChain
	UserData    *UserData
}

func (r *RequestStream) addMeta(id string, content []byte) []byte {
	var meta_buf bytes.Buffer
	meta_writer := bufio.NewWriter(&meta_buf)

	//enc := xml.NewEncoder(os.Stdout)
	//enc := xml.NewEncoder(meta_buf)
	enc := xml.NewEncoder(meta_writer)
	data := &MetaData{
		TaskIdChain: &TaskIdChain{
			TaskId: []string{id},
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
		len(id), id, meta_buf.Len())

	meta_writer.WriteString(fmt.Sprintf("TASK%08x", len(content)))
	meta_writer.Write(content)
	meta_writer.WriteString("FFFFFFFF")
	meta_writer.Flush()

	meta_buf.Bytes()
	all_bytes := []byte(payload)
	all_bytes = append(all_bytes, meta_buf.Bytes()[:]...)
	// correlate msg.Id & task id in log
	r.Log.Debug("addMeta ok", "msg", id, "all_len", len(all_bytes))
	return all_bytes
}

func (r *RequestStream) Get() (gentle.Message, error) {
	id := xid.New().String()
	mailId := r.names[r.gen.Intn(len(r.names))]
	content := r.addMeta(id, r.requests[mailId])
	// correlate msg.Id & mailId in log
	r.Log.Debug("Get() ok", "msg", id, "mail_id", mailId,
		"mail_len", len(content))
	return &hesScanReq{
		id:      id,
		mailId:  mailId,
		content: content,
	}, nil
}

func runOne() {
	scans := NewRequestStream(*dir)
	msg, _ := scans.Get()
	hreq := msg.(*hesScanReq)
	if *isDryRun {
		fmt.Println("Is a dry run. Exit")
		return
	}

	csvWriter, err := createCsvWriter(*csvFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	csvChan := createCsvChannel(csvWriter, *csvBufRows,
		time.Duration(*csvFlush)*time.Second)
	hes := NewHesSendHandler(*url+"/scanner/mail", csvChan)
	hmsg, err := hes.Handle(hreq)
	if err != nil {
		log.Error("Handle err", "err", err)
	}
	hresp := hmsg.(*hesScanResp)
	fmt.Println(string(hresp.content))
}

func runLoop() {
	csvWriter, err := createCsvWriter(*csvFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	csvChan := createCsvChannel(csvWriter, *csvBufRows,
		time.Duration(*csvFlush)*time.Second)
	scans := NewRequestStream(*dir)
	var handler gentle.Handler
	interval := 1000 / (*maxScansSec)
	if interval > 0 {
		log.Debug("Rate limit enabled, pause in millis between every scan",
			"pause", interval)
		handler = gentle.NewRateLimitedHandler("sense",
			NewHesSendHandler(*url+"/scanner/mail", csvChan),
			gentle.NewTokenBucketRateLimit(interval, 1))
	} else {
		handler = NewHesSendHandler(*url+"/scanner/mail", csvChan)
	}
	stream := gentle.NewMappedStream("sense", scans, handler)
	runStream(stream, *maxConcurrency, *maxScans)
}

func runStream(stream gentle.Stream, concurrent_num int, count int64) {
	var total int64
	var success_total int64

	result := make(chan *struct{}, concurrent_num)
	for i := int64(0); i < count; i++ {
		result <- &struct{}{}
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

func createCsvWriter(filename string) (*csv.Writer, error) {
	columns := []string{"task_id", "mail_id", "mail_sz",
		"scan_req_begin", "scan_req_dura", "scan_req_len",
		"scan_resp_status", "scan_resp_hes_state",
		"scan_resp_hes_in", "scan_resp_hes_out", "scan_resp_hes_doing"}

	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	csvWriter := csv.NewWriter(bufio.NewWriter(f))
	if err := csvWriter.Write(columns); err != nil {
		os.Remove(filename)
		return nil, err
	}
	// Make sure the row of column names appear before anything else
	csvWriter.Flush()
	return csvWriter, nil
}

func createCsvChannel(csvWriter *csv.Writer, numBufRow int, flush time.Duration) chan []string {
	rowChan := make(chan []string, numBufRow)
	tk := time.NewTicker(flush)
	go func() {
		for {
			select {
			case row := <-rowChan:
				err := csvWriter.Write(row)
				if err != nil {
					log.Debug("csv Write err", "msg_in", row[0], "err", err)
				} else {
					log.Debug("csv Write ok", "msg_in", row[0])
				}
			case <-tk.C:
				csvWriter.Flush()
			}
		}
	}()
	return rowChan
}

func main() {
	h := log15.MultiHandler(log15.StdoutHandler,
		log15.Must.FileHandler(*logFile, log15.LogfmtFormat()))
	log.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, h))
	gentle.Log.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, h))

	//runOne()
	runLoop()

	// block until keyboard interrupt
	var block <-chan *struct{}
	<-block
}
