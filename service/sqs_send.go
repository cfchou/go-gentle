package service

import (
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/afex/hystrix-go/hystrix"
	//"github.com/afex/hystrix-go/plugins"
	"time"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
)

var ErrRateLimited = errors.New("Rate limit reached")

type SqsSendService struct {
	Name string
	Conf SqsSendServiceConfig

	client sqsiface.SQSAPI
	limiter *RateLimiter
}

type SqsSendServiceConfig struct {
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

	RateLimitConf          RateLimitConfig
}

// There're structures that are modified from aws Input/Output types.
// In them, pointers are often used instead of primitive values.
// That's mainly for identifying zero values from fields not set.

// Modified from sqs.SendMessageInput
type SendSpec struct {
	DelaySeconds *int64
	MessageAttributes map[string]*attrSpec
	MessageBody *string
	MessageDeduplicationId *string
	MessageGroupId *string
}

// Simplified sqs.MessageAttributeValue, fields for lists of data are removed.
type attrSpec struct {
	// String, Number or Binary
	DataType *string
	StringValue *string
	BinaryValue []byte
}

func (conf *SqsSendServiceConfig) createSendMessageInput(spec *SendSpec) (*sqs.SendMessageInput, error) {
	var attrs map[string]*sqs.MessageAttributeValue
	for k, v := range spec.MessageAttributes {
		attrs[k] = &sqs.MessageAttributeValue{
			DataType: v.DataType,
			StringValue: v.StringValue,
			BinaryValue: v.BinaryValue,
		}
	}

	input := &sqs.SendMessageInput{
		QueueUrl: aws.String(conf.Url),
		DelaySeconds: spec.DelaySeconds,
		MessageAttributes: attrs,
		MessageBody: spec.MessageBody,
		MessageDeduplicationId: spec.MessageDeduplicationId,
		MessageGroupId: spec.MessageGroupId,
	}
	err := input.Validate()
	if err != nil {
		return nil, err
	}
	return input, nil
}

func (conf *SqsSendServiceConfig) NewSqsSendService(name string, client sqsiface.SQSAPI) *SqsSendService {
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
		Conf: *conf,
		client: client,
		limiter: conf.RateLimitConf.NewRateLimit(),
	}
}

func (q *SqsSendService) SendMessage(spec SendSpec, timeout time.Duration) (*sqs.SendMessageOutput, error) {
	input, err := q.Conf.createSendMessageInput(&spec)
	if err != nil {
		return nil, err
	}
	// blocked
	if !q.limiter.WaitMaxDuration(1, timeout) {
		return nil, ErrRateLimited
	}

	result := make(chan *sqs.SendMessageOutput, 1)
	err2 := hystrix.Do(q.Name, func () (error) {
		resp, err := q.client.SendMessage(input)
		if err != nil {
			return err
		}
		result <- resp
		return nil
	}, func (err error) error {
		// log
		return err
	})
	if err2 != nil {
		return nil, err
	}
	return <- result, nil
}

