package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"github.com/hashicorp/go-cleanhttp"
	"github.com/rs/xid"
	"github.com/spf13/pflag"
	"gopkg.in/cfchou/go-gentle.v1/gentle"
	"gopkg.in/inconshreveable/log15.v2"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const max_int64 = int64(^uint64(0) >> 1)

var (
	log = log15.New("mixin", "main")

	// command line options
	url            = pflag.String("url", "http://localhost:8080", "HES url")
	csvFile        = pflag.String("csv-file", "esnes.csv", "csv filename")
	csvBufRows      = pflag.Int("csv-buf-rows", 2048, "max buffered rows in csv befor write")
	csvFlush      = pflag.Int("csv-flush", 3, "csv flush interval in secs")
	logFile        = pflag.String("log-file", "esnes.log", "log filename")
	maxConcurrency = pflag.Int("max-concurrency", 1, "max concurrent requests")
	maxRecvs       = pflag.Int64("max-recvs", max_int64, "max recv requests to HES")
	maxRecvsSec    = pflag.Int("max-recvs-sec", 2, "rate limit of max recv per second")
	popCount       = pflag.Int("pop-count", 51, "HES pop count")
	popSize        = pflag.Int64("pop-size", 16777216, "HES pop size")
)

func init() {
	pflag.Parse()
}

type HesRecvStream struct {
	Log       log15.Logger
	client    *http.Client
	url       string
	csvChan	chan []string
	body      string
}

const recv_format = `<p1:common_pop_request xmlns:p1="http://www.trendmicro.com/nebula/xml_schema">
<pop_count>%d</pop_count>
<pop_size>%d</pop_size>
<dest_host>10.64.70.20</dest_host>
</p1:common_pop_request>`

func NewHesRecvStream(url string, csvChan chan []string) *HesRecvStream {
	return &HesRecvStream{
		Log:       log.New("mixin", "hes_send"),
		client:    cleanhttp.DefaultPooledClient(),
		url:       url,
		csvChan: csvChan,
		body:      fmt.Sprintf(recv_format, *popCount, *popSize),
	}
}

func (s *HesRecvStream) Get() (gentle.Message, error) {

	req, err := http.NewRequest(http.MethodPut, s.url, strings.NewReader(s.body))
	if err != nil {
		s.Log.Error("NewRequest err", "err", err)
		return nil, err
	}

	begin := time.Now()
	resp, err := s.client.Do(req)
	if err != nil {
		s.Log.Error("PUT err", "err", err)
		return nil, err
	}
	timespan := time.Now().Sub(begin)
	batchId := xid.New().String()

	if resp.StatusCode == http.StatusNoContent || resp.StatusCode < 200 ||
		resp.StatusCode >= 300 {
		s.Log.Warn("PUT returns suspicious status",
			"status", resp.Status)
		// Not treated as error
		return &hesRecvResp{
			id:      batchId,
			TaskIds: []string{},
		}, nil
	}

	s.Log.Debug("PUT timespan", "status", resp.Status,
		"begin", begin.Format(time.StampMilli),
		"timespan", timespan)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &hesRecvResp{
			id:      batchId,
			TaskIds: []string{},
		}, nil
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		s.Log.Error("ReadAll() err", "err", err)
		return nil, err
	}
	s.Log.Debug("ReadAll() ok", "body_len", len(content))

	// ["task_id" will be added] "recv_req_begin", "recv_req_dura", "recv_req_bid"
	row := []string{
		strconv.FormatInt(begin.Unix(), 10),
		strconv.FormatFloat(timespan.Seconds(), 'f', 3, 64),
		batchId,
	}
	taskIds := s.parseContent(content, row)
	for _, tid := range taskIds {
		log.Debug("parseContent", "msg", batchId, "task_id", tid,
			"count", len(taskIds))
	}
	return &hesRecvResp{
		id:      batchId,
		TaskIds: taskIds,
	}, nil
}

func (s *HesRecvStream) parseContent(content []byte, row []string) []string {
	rest := content
	minimum := 3*8 + len("00000002META") + len("TASK")
	taskIds := []string{}
	for {
		if len(rest) == 8 && string(rest) == "FFFFFFFF" {
			s.Log.Debug("Parse done", "count", len(taskIds))
			return taskIds
		}
		if len(rest) <= minimum {
			s.Log.Warn("Incomplete content")
			return taskIds
		}
		var taskIdLen, metaLen, dataLen int
		n, err := fmt.Sscanf(string(rest[:8]), "%08x", &taskIdLen)
		if err != nil || n != 1 {
			s.Log.Error("Sscanf taskIdLen err",
				"invalid", string(rest[:8]))
			return taskIds
		}
		expectLen := 8 + taskIdLen + len("00000002META")
		if len(rest) <= expectLen+8 {
			s.Log.Warn("Incomplete content")
			return taskIds
		}
		taskId := string(rest[8 : 8+taskIdLen])
		taskIds = append(taskIds, taskId)

		rest = rest[expectLen:]
		n, err = fmt.Sscanf(string(rest[:8]), "%08x", &metaLen)
		if err != nil || n != 1 {
			s.Log.Error("Sscanf metaLen err",
				"invalid", string(rest[:8]))
			return taskIds
		}
		expectLen = 8 + metaLen + len("TASK")
		if len(rest) <= expectLen+8 {
			s.Log.Warn("Incomplete content")
			return taskIds
		}
		// meta := string(rest[8:8+metaLen])

		rest = rest[expectLen:]
		n, err = fmt.Sscanf(string(rest[:8]), "%08x", &dataLen)
		if err != nil || n != 1 {
			s.Log.Error("Sscanf dataLen err",
				"invalid", string(rest[:8]))
			return taskIds
		}
		expectLen = 8 + dataLen
		if len(rest) < expectLen+8 {
			s.Log.Warn("Incomplete content")
			return taskIds
		}
		//data := string(rest[8:8+dataLen])

		rest = rest[expectLen:]

		fullRow := []string{taskId}
		fullRow = append(fullRow, row...)
		s.csvChan <- fullRow
	}
}

type hesRecvResp struct {
	id      string
	TaskIds []string
}

func (m *hesRecvResp) Id() string {
	return m.id
}

func runLoop() {
	csvWriter, err := createCsvWriter(*csvFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	csvChan := createCsvChannel(csvWriter, *csvBufRows,
		time.Duration(*csvFlush) * time.Second)
	var stream gentle.Stream
	interval := 1000 / (*maxRecvsSec)
	if interval > 0 {
		log.Debug("Rate limit enabled, pause in millis between every scan",
			"pause", interval)
		stream = gentle.NewRateLimitedStream("esnes",
			NewHesRecvStream(*url+"/scanner/deliver", csvChan),
			gentle.NewTokenBucketRateLimit(interval, 1))
	} else {
		stream = NewHesRecvStream(*url+"/scanner/deliver", csvChan)
	}
	runStream(stream, *maxConcurrency, *maxRecvs)
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
	columns := []string{"task_id", "recv_req_begin", "recv_req_dura",
		"recv_req_bid"}

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
			case row := <- rowChan:
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

	runLoop()

	// block until keyboard interrupt
	var block <-chan *struct{}
	<-block
}
