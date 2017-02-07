package service

import (
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/afex/hystrix-go/hystrix"
	"time"
	"errors"
	"github.com/inconshreveable/log15"
)

var ErrRateLimited = errors.New("Rate limit reached")
var ErrTimeout = errors.New("Timeout")

type SqsSendService struct {
	Name string
	Conf *SqsSendServiceConf

	log log15.Logger
	client sqsiface.SQSAPI
	limiter RateLimit
}

type SqsSendServiceConf struct {
	Url                    string `json:"url"`

	// Timeout is how long to wait for sqs.SendMessage() to complete,
	// in milliseconds.
	Timeout                int `json:"timeout"`

	// MaxConcurrentRequests is how many sqs.SendMessage() can run at the same
	// time.
	MaxConcurrentRequests  int `mapstructure:"max_concurrent_requests", json:"max_concurrent_requests"`

	// RequestVolumeThreshold is the minimum number of requests in the past 10
	// seconds needed before the failed rate calculation kicks in.
	// The failed rate calculation is based on the number of of requests in
	// the past 10 seconds.
	RequestVolumeThreshold int `mapstructure:"request_volume_threshold", json:"request_volume_threshold"`

	// The circuit is opened if the railed rate reaches
	// ErrorPercentThreshold.
	ErrorPercentThreshold  int `mapstructure:"error_percent_threshold", json:"error_percent_threshold"`

	// SleepWindow is how long, in milliseconds, to wait after the
	// circuit opens before testing for recovery
	SleepWindow            int `mapstructure:"sleep_window", json:"sleep_window"`
}

type SendSpec interface {
	ToSendMessageInput() (*sqs.SendMessageInput, error)
}

type SendInput struct {
	sqs.SendMessageInput
}

func (spec *SendInput) ToSendMessageInput() (*sqs.SendMessageInput, error) {
	return &spec.SendMessageInput, nil
}

func NewSqsSendService(name string, conf SqsSendServiceConf,
	client sqsiface.SQSAPI,rate_limiter RateLimit) *SqsSendService {

	// A circuit breaker for sqs.SendMessage()
	hystrix.ConfigureCommand(name, hystrix.CommandConfig{
		Timeout: conf.Timeout,
		MaxConcurrentRequests: conf.MaxConcurrentRequests,
		RequestVolumeThreshold: conf.RequestVolumeThreshold,
		ErrorPercentThreshold: conf.ErrorPercentThreshold,
		SleepWindow: conf.SleepWindow,
	})
	return &SqsSendService {
		Name: name,
		Conf: &conf,
		log: Log.New("service", name),
		client: client,
		limiter: rate_limiter,
	}
}

func (s *SqsSendService) SendMessage(spec SendSpec, timeout time.Duration) (*sqs.SendMessageOutput, error) {
	// TODO timeout doesn't apply to client.SendMessage()
	/*
	spec, ok := msg.(SendInput)
	if !ok {
		return nil,
	}
	*/
	input, _ := spec.ToSendMessageInput()
	input.SetQueueUrl(s.Conf.Url)
	if err := input.Validate(); err != nil {
		return nil, err
	}

	// Blocked
	s.log.Debug("[SqsSend] Wait")
	if !s.limiter.Wait(1, timeout) {
		s.log.Warn("[SqsSend]", "err", ErrRateLimited)
		return nil, ErrRateLimited
	}

	result := make(chan *sqs.SendMessageOutput, 1)

	err := hystrix.Do(s.Name, func () (error) {
		resp, err := s.client.SendMessage(input)
		if err != nil {
			s.log.Error("[SqsSend] SendMessage err", "err", err)
			return err
		}
		s.log.Debug("[SqsSend] SendMessage ok")
		result <- resp
		return nil
	}, nil)

	if err != nil {
		// if err == hystrix.ErrTimeout, the msg might still be sent.
		s.log.Error("[SqsSend] Err due to", "err", err)
		return nil, err
	}
	return <- result, nil
}





