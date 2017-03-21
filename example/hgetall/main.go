package main

import (
	"gopkg.in/redis.v5"
	"github.com/spf13/pflag"
	"fmt"
	"encoding/csv"
	"bufio"
	"os"
)
var (
	db              = pflag.Int("redis-db", 0, "the db used in redis")
)

func init() {
	pflag.Parse()
}

func encodeCSV(columns []string, rows []map[string]string) error {
	//var buf bytes.Buffer
	f, err := os.Create("hgetall.csv")
	if err != nil {
		return err
	}
	csvWriter := csv.NewWriter(bufio.NewWriter(f))
	if err := csvWriter.Write(columns); err != nil {
		return err
	}
	r := make([]string, len(columns))
	for _, row :=  range rows {
		for i, column := range columns {
			r[i] = row[column]
		}
		if err := csvWriter.Write(r); err != nil {
			return err
		}
	}
	csvWriter.Flush()
	return nil
}

func main() {

	rd := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       *db,  // use default DB
	})
	keys, err := rd.Keys("*").Result()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("Total %d keys\n", len(keys))

	/*
	"scan_req_dura","0.531","scan_resp_status","200 OK",
	"scan_resp_hes_state","2","scan_resp_hes_in","1","scan_resp_hes_out","1",
	"mail_id","15adae55b4304dad","scan_mail_sz","14175","scan_req_len","14175",
	"scan_req_begin","1490114338","scan_resp_hes_doing","1",
	"recv_req_begin","1490114344","recv_req_dura","0.719",
	"recv_req_bid","b38leabsfb610uvjsirg"
	 */
	columns := []string{"task_id", "mail_id", "scan_mail_sz",
		"scan_req_begin", "scan_req_dura", "scan_req_len",
		"scan_resp_status", "scan_resp_hes_state",
		"scan_resp_hes_in", "scan_resp_hes_out", "scan_resp_hes_doing",
		"recv_req_begin", "recv_req_dura", "recv_req_bid"}

	rows := []map[string]string{}
	for _, key := range keys {
		kv, err := rd.HGetAll(key).Result()
		if err != nil {
			fmt.Printf("HMGet %s, err: %v\n", key, err)
			return
		}
		kv["task_id"] = key
		rows = append(rows, kv)
	}

	encodeCSV(columns, rows)
}
