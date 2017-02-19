// vim:fileencoding=utf-8
package service

import (
	"time"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"sync"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/inconshreveable/log15"
	"errors"
	"gopkg.in/eapache/go-resiliency.v1/retrier"
	"github.com/cfchou/porter/service"
	"fmt"
)

type SqsSource struct {
	client sqsiface.SQSAPI
	input *sqs.ReceiveMessageInput
}

func NewSqsSource(client sqsiface.SQSAPI, input sqs.ReceiveMessageInput) *SqsSource {
	return &SqsSource{
		client: client,
		input:  &input,
	}
}

func (r *SqsSource) ReceiveMessages() ([]service.Message, error) {
	resp, err := r.client.ReceiveMessage(r.input)
	if err != nil {
		return nil, err
	}
	msgs := make([]service.Message, len(resp.Messages))
	for i, msg := range resp.Messages {
		msgs[i] = NewSqsMessage(msg)
	}
	return msgs, nil
}



type SqsUpStreamConf struct {
	// MaxWaitingMessages is the max number of messages that have not been
	// removed by WaitMessage(). It's the capacity of the buffered channel
	// SqsService.queue.
	MaxWaitingMessages int `mapstructure:"max_waiting_messages", json:"max_waiting_messages"`

	// RequestVolumeThreshold is the minimum number of requests in the past 10
	// seconds needed before the failed rate calculation kicks in.
	// The failed rate calculation is based on the number of of requests in
	// the past 10 seconds.
	RequestVolumeThreshold int `mapstructure:"request_volume_threshold", json:"request_volume_threshold"`

	// The circuit is opened if the railed rate reaches
	// ErrorPercentThreshold.
	ErrorPercentThreshold int `mapstructure:"error_percent_threshold", json:"error_percent_threshold"`

	// SleepWindow is the minimum time of how long, in milliseconds, to wait
	// after the circuit opens before testing for recovery
	SleepWindow int `mapstructure:"sleep_window", json:"sleep_window"`
}

type SqsUpStream struct {
	Name string
	Conf *SqsUpStreamConf
	service.UpStream
}

func NewSqsUpStream(name string, conf SqsUpStreamConf, client SqsSource,
	monitor service.Monitor, backOff service.BackOff) *SqsUpStream {

	// Register a circuit breaker for sqs.ReceiveMessage()
	hystrix.ConfigureCommand(name, hystrix.CommandConfig{
		// Long polling is supported by sqs. A valid WaitTimeSeconds
		// is from 0 to 20. Set Timeout to be larger than 20 to avoid
		// clashing with WaitTimeSeconds.
		Timeout: 30000,
		// This command is run exclusively by this service and
		// sqs.ReceiveMessage() is always called sequentially.
		MaxConcurrentRequests:  1,
		RequestVolumeThreshold: conf.RequestVolumeThreshold,
		ErrorPercentThreshold:  conf.ErrorPercentThreshold,
		SleepWindow:            conf.SleepWindow,
	})

	up := service.NewDefaultBackPressuredUpStream(name,
		conf.MaxWaitingMessages, client, monitor, backOff)

	return &SqsUpStream{
		Name:      name,
		UpStream:  up,
		Conf:      &conf,
	}
}

