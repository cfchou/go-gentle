// vim:fileencoding=utf-8
package service

import (
	"testing"
	"github.com/stretchr/testify/mock"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/stretchr/testify/assert"
	"github.com/aws/aws-sdk-go/aws"
)

type MockClient struct {
	mock.Mock
	sqsiface.SQSAPI
}

func (c *MockClient) ReceiveMessage(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	args := c.Called(input)
	return args.Get(0).(*sqs.ReceiveMessageOutput), args.Error(1)
}

func TestA(t *testing.T) {

	conf := &SqsReceiveServiceConfig{
		Url:"http://some.endpoint",
		MaxWaitingMessages:10,
		RequestVolumeThreshold:10,
		ErrorPercentThreshold:10,
		SleepWindow:3000,
	}

	mc := &MockClient{}
	call := mc.On("ReceiveMessage")
	call.Return()

	spec := ReceiveSpec{
		WaitTimeSeconds: aws.Int64(2),
	}

	svc, err := conf.NewSqsReceiveService("test_recv", mc, spec)
	assert.Nil(t, err)
	svc.Run()


}
