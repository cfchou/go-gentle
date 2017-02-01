package main

import (
	"encoding/json"
	"github.com/spf13/viper"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"errors"
	"github.com/cfchou/porter/service"
)

var default_conf = []byte(`
{
	"sqs": {
		"recv": {
			"url": "http://endpoint.sqs.aws.com",
			"max_waiting_messages": 16,
			"request_volume_threshold": 10,
			"error_percent_threshold": 50,
			"sleep_window": 120000
		},
		"send": {
			"url": "http://www.msn.com",
			"timeout": 1
		}
	}
}
`)

type TyA struct {
	Url string
	MaxWaitingMessages     int `mapstructure:"max_waiting_messages"`
	ReceiveSpec TyB `mapstructure:"receive_spec"`
}

/*
type ReceiveSpec struct {
	AttributeNames []*string
	MaxNumberOfMessages *int64
	MessageAttributeNames []*string
	ReceiveRequestAttemptId *string
	VisibilityTimeout *int64
	WaitTimeSeconds *int64
}
*/

type TyB struct {
	Url *string
	Timeout int
}

func setupViper() error {
	// setup default
	var conf interface{}
	err := json.Unmarshal(default_conf, &conf)
	if err != nil {
		return err
	}
	m, ok := conf.(map[string]interface{})
	if !ok {
		return errors.New("can't convert")
	}
	viper.SetDefault("sqs", m["sqs"])

	// setup default
	viper.SetConfigName("porter")
	viper.AddConfigPath("/etc/porter")
	viper.AddConfigPath(".")
	err = viper.ReadInConfig()
	if err != nil {
		return err
	}
	return nil
}

func main() {
	fmt.Println("start")
	err := setupViper()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	var conf_r service.SqsReceiveServiceConfig
	viper.UnmarshalKey("sqs.recv", &conf_r)
	spew.Dump(conf_r)
	bs, err := json.Marshal(conf_r)
	if err != nil {
		panic(fmt.Errorf("json.Marshal failed %s \n", err))
	}
	spew.Dump(bs)

	fmt.Println("exit")
	/*
	var sqs_r SqsReceiveServiceConfig
	err = viper.UnmarshalKey("sqs.recv", &sqs_r)
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	*/
}
