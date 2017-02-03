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
	"github.com/inconshreveable/log15"
	"flag"
	"errors"
)

func TestMain(m *testing.M) {
	flag.Parse()
	//h := log15.LvlFilterHandler(log15.LvlDebug, log15.CallerFuncHandler(log15.StdoutHandler))
	h := log15.LvlFilterHandler(log15.LvlDebug,
			log15.MultiHandler(
				log15.StdoutHandler,
				log15.Must.FileHandler("./test.log", log15.LogfmtFormat())))
	Log.SetHandler(h)
	m.Run()
}

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

func _TestSqsReceiveService_WaitMessage_One(t *testing.T) {
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
	name := fmt.Sprintf("WaitMessage_One_%d", rand.Int63n(time.Now().Unix()))
	svc, err := fake_conf.NewSqsReceiveService(name, mc, mspec)
	assert.Nil(t, err)
	svc.Run()
	msg, _ := svc.WaitMessage(0)
	assert.EqualValues(t, *msg, *fake_output.Messages[0])
}

func _TestSqsReceiveService_WaitMessage_OneMulti(t *testing.T) {
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
	name := fmt.Sprintf("WaitMessage_OneMulti_%d", rand.Int63n(time.Now().Unix()))
	svc, err := fake_conf.NewSqsReceiveService(name, mc, mspec)
	assert.Nil(t, err)
	svc.Run()

	for i := 1; i <= count; i++ {
		Log.Debug("[test]", "service", name, "num", i, "count", count)
		msg, _ := svc.WaitMessage(0)
		assert.EqualValues(t, *msg, *fake_output.Messages[0])
	}
}

func _TestSqsReceiveService_WaitMessage_Many(t *testing.T) {
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

	name := fmt.Sprintf("WaitMessage_Many_%d", rand.Int63n(time.Now().Unix()))
	svc, err := fake_conf.NewSqsReceiveService(name, mc, mspec)
	assert.Nil(t, err)
	svc.Run()

	for i := 0; i < many; i++ {
		msg, _ := svc.WaitMessage(0)
		assert.EqualValues(t, *msg, *fake_output.Messages[i])
	}
}

func _TestSqsReceiveService_WaitMessage_ManyMulti(t *testing.T) {
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

	name := fmt.Sprintf("WaitMessage_ManyMulti_%d", rand.Int63n(time.Now().Unix()))
	//t.Logf("Testing %s......", name)
	svc, err := fake_conf.NewSqsReceiveService(name, mc, mspec)
	assert.Nil(t, err)
	svc.Run()

	for i := 1; i <= count; i++ {
		Log.Debug("[test]", "service", name, "num", i, "count", count)
		for j := 0; j < many; j++ {
			msg, _ := svc.WaitMessage(0)
			assert.EqualValues(t, *msg, *fake_output.Messages[j])
		}
	}
}

// Normal flow
func _TestSqsReceiveService_RunWithBackPressure(t *testing.T) {
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

func TestSqsReceiveService_RunWithBackPressure_1(t *testing.T) {
	fake_url := "http://some.endpoint"
	fake_conf := fakeSqsReceiveServiceConfig(fake_url)
	fake_conf.MaxWaitingMessages = 3
	fake_input := sqs.ReceiveMessageInput{}
	fake_output := fakeReceiveMessageOutput(1)
	//name := fmt.Sprintf("RunWithBackPressure_%d", rand.Int63n(time.Now().Unix()))
	name := ""
	fake_bpconf := BackPressureConf{
		Name: fmt.Sprintf("%s_bp", name),
		// allow a slow(bounded by 100sec) handler
		Timeout: 100000,
		// make the circuit open
		RequestVolumeThreshold: 1,
		ErrorPercentThreshold: 1,
		SleepWindow: 3000,
		BackoffExpInit: 1000,
		BackoffExpSteps: 3,
		BackoffConstSleepWindow: 5000,
		BackoffConstSteps: 3,
	}

	mspec := &MockSpec{}
	mspec.On("ToReceiveMessageInput", fake_url).
		Return(&fake_input, nil)

	//done := make(chan *struct{}, 1)
	run_times := 0
	mc := &MockClient{}
	mc.On("ReceiveMessage", &fake_input).
		Run(func(args mock.Arguments) {
			//done <- &struct {}{}
			run_times++
			Log.Debug("[test] message", "run_times", run_times)
		}).
		Return(fake_output, nil)

	svc, err := fake_conf.NewSqsReceiveService(name, mc, mspec)
	assert.Nil(t, err)

	handle_done := make(chan *struct{}, 1)
	handle_times := 0
	handler := func (msg *sqs.Message) error {
		assert.EqualValues(t, *msg, *fake_output.Messages[0])
		time.Sleep(time.Duration(2) * time.Second)
		handle_times++
		Log.Debug("[test] handler", "handle_times", handle_times)
		if handle_times > 5 {
			if handle_times > 20 {
				handle_done <- &struct{}{}
			}
			return nil
		}
		return errors.New("Test handler fail")
	}
	svc.RunWithBackPressure(fake_bpconf, handler)
	<- handle_done
	Log.Debug("[test]", "service", name, "run_times", run_times, "handle_times", handle_times)

}
