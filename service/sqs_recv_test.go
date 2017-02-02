// vim:fileencoding=utf-8
package service

import (
	"testing"
	"github.com/stretchr/testify/mock"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/stretchr/testify/assert"
	"fmt"
	"math/rand"
	"time"
	"log"
)


type MockClient struct {
	mock.Mock
	sqsiface.SQSAPI
}

func (m *MockClient) ReceiveMessage(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*sqs.ReceiveMessageOutput), args.Error(1)
}

type MockSpec struct {
	mock.Mock
	ReceiveSpec
}

func (m *MockSpec) ToReceiveMessageInput(url string) (*sqs.ReceiveMessageInput, error) {
	args := m.Called(url)
	return args.Get(0).(*sqs.ReceiveMessageInput), args.Error(1)
}

func fakeSqsReceiveServiceConfig(url string) *SqsReceiveServiceConfig {
	return &SqsReceiveServiceConfig{
		Url: url,
		MaxWaitingMessages: 10,
		RequestVolumeThreshold: 10,
		ErrorPercentThreshold: 10,
		SleepWindow: 10,
	}
}

func fakeReceiveMessageOutput(n int) (*sqs.ReceiveMessageOutput) {
	ret := &sqs.ReceiveMessageOutput{}
	var msgs []*sqs.Message
	for i := 0; i < n; i++ {
		dummy := fmt.Sprintf("dummy_%d", i)
		msg := &sqs.Message{}
		msg.SetBody(dummy)
		msg.SetReceiptHandle(dummy)
		msgs = append(msgs, msg)
	}
	ret.SetMessages(msgs)
	return ret
}

func TestSqsReceiveService_WaitMessage_One(t *testing.T) {
	fake_url := "http://some.endpoint"
	fake_conf := fakeSqsReceiveServiceConfig(fake_url)
	fake_input := sqs.ReceiveMessageInput{}
	fake_output := fakeReceiveMessageOutput(1)

	mspec := &MockSpec{}
	mspec.On("ToReceiveMessageInput", fake_url).
		Return(&fake_input, nil)

	// Calls except for the first one will be blocked
	done := make(chan *struct{}, 1)
	mc := &MockClient{}
	mc.On("ReceiveMessage", &fake_input).
		Run(func(args mock.Arguments) {
			done <- &struct {}{}
		}).
		Return(fake_output, nil)

	// Must give a different name across all tests to prevent clashes of the circuit breakers.
	name := fmt.Sprintf("WaitMessage_One_%d", rand.Int63n(time.Now().UnixNano()))
	svc, err := fake_conf.NewSqsReceiveService(name, mc, mspec)
	assert.Nil(t, err)
	svc.Run()
	msg, _ := svc.WaitMessage(0)
	assert.EqualValues(t, *msg, *fake_output.Messages[0])
}

func TestSqsReceiveService_WaitMessage_OneMulti(t *testing.T) {
	count := 5
	fake_url := "http://some.endpoint"
	fake_conf := fakeSqsReceiveServiceConfig(fake_url)
	fake_input := sqs.ReceiveMessageInput{}
	fake_output := fakeReceiveMessageOutput(1)

	mspec := &MockSpec{}
	mspec.On("ToReceiveMessageInput", fake_url).
		Return(&fake_input, nil)

	// Calls after $count times will be blocked
	done := make(chan *struct{}, count)
	mc := &MockClient{}
	mc.On("ReceiveMessage", &fake_input).
		Run(func(args mock.Arguments) {
			done <- &struct {}{}
		}).
		Return(fake_output, nil)

	// Must give a different name across all tests to prevent clashes of the circuit breakers.
	name := fmt.Sprintf("WaitMessage_OneMulti_%d", rand.Int63n(time.Now().UnixNano()))
	svc, err := fake_conf.NewSqsReceiveService(name, mc, mspec)
	assert.Nil(t, err)
	svc.Run()

	for i := 1; i <= count; i++ {
		log.Printf("[%s] count: %d out %d", name, i, count)
		msg, _ := svc.WaitMessage(0)
		assert.EqualValues(t, *msg, *fake_output.Messages[0])
	}
}

func TestSqsReceiveService_WaitMessage_Many(t *testing.T) {
	many := 10
	fake_url := "http://some.endpoint"
	fake_conf := fakeSqsReceiveServiceConfig(fake_url)
	fake_input := sqs.ReceiveMessageInput{}
	fake_output := fakeReceiveMessageOutput(many)

	mspec := &MockSpec{}
	mspec.On("ToReceiveMessageInput", fake_url).
		Return(&fake_input, nil)

	done := make(chan *struct{}, 1)
	mc := &MockClient{}
	mc.On("ReceiveMessage", &fake_input).
		Run(func(args mock.Arguments) {
			done <- &struct {}{}
		}).
		Return(fake_output, nil)

	name := fmt.Sprintf("WaitMessage_Many_%d", rand.Int63n(time.Now().UnixNano()))
	//t.Logf("Testing %s......", name)
	svc, err := fake_conf.NewSqsReceiveService(name, mc, mspec)
	assert.Nil(t, err)
	svc.Run()

	for i := 0; i < many; i++ {
		msg, _ := svc.WaitMessage(0)
		assert.EqualValues(t, *msg, *fake_output.Messages[i])
	}
}

func TestSqsReceiveService_WaitMessage_ManyMulti(t *testing.T) {
	count := 5
	many := 5
	fake_url := "http://some.endpoint"
	fake_conf := fakeSqsReceiveServiceConfig(fake_url)
	fake_input := sqs.ReceiveMessageInput{}
	fake_output := fakeReceiveMessageOutput(many)

	mspec := &MockSpec{}
	mspec.On("ToReceiveMessageInput", fake_url).
		Return(&fake_input, nil)

	done := make(chan *struct{}, count)
	mc := &MockClient{}
	mc.On("ReceiveMessage", &fake_input).
		Run(func(args mock.Arguments) {
			done <- &struct {}{}
		}).
		Return(fake_output, nil)

	name := fmt.Sprintf("WaitMessage_ManyMulti_%d", rand.Int63n(time.Now().UnixNano()))
	//t.Logf("Testing %s......", name)
	svc, err := fake_conf.NewSqsReceiveService(name, mc, mspec)
	assert.Nil(t, err)
	svc.Run()

	for i := 1; i <= count; i++ {
		log.Printf("[%s] count: %d out %d", name, i, count)
		for j := 0; j < many; j++ {
			msg, _ := svc.WaitMessage(0)
			assert.EqualValues(t, *msg, *fake_output.Messages[j])
		}
	}
}

func TestSqsReceiveService_RunWithBackPressure(t *testing.T) {
	fake_url := "http://some.endpoint"
	fake_conf := fakeSqsReceiveServiceConfig(fake_url)
	fake_input := sqs.ReceiveMessageInput{}
	fake_output := fakeReceiveMessageOutput(1)
	name := fmt.Sprintf("RunWithBackPressure_%d", rand.Int63n(time.Now().Unix()))
	fake_bpconf := BackPressureConf{
		Name: fmt.Sprintf("%s_bp", name),
		// keep the circuit closed
		Timeout: 10000,
		RequestVolumeThreshold: 1000,
		ErrorPercentThreshold: 100,
		SleepWindow: 10,
	}

	mspec := &MockSpec{}
	mspec.On("ToReceiveMessageInput", fake_url).
		Return(&fake_input, nil)

	// Calls except for the first one will be blocked
	done := make(chan *struct{}, 1)
	mc := &MockClient{}
	mc.On("ReceiveMessage", &fake_input).
		Run(func(args mock.Arguments) {
			done <- &struct {}{}
		}).
		Return(fake_output, nil)

	svc, err := fake_conf.NewSqsReceiveService(name, mc, mspec)
	assert.Nil(t, err)

	handle_done := make(chan *struct{}, 1)
	handler := func (msg *sqs.Message) error {
		assert.EqualValues(t, *msg, *fake_output.Messages[0])
		handle_done <- &struct{}{}
		return nil
	}
	svc.RunWithBackPressure(fake_bpconf, handler)
	<- handle_done
}
