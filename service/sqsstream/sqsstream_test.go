// vim:fileencoding=utf-8
package service

import (
	"flag"
	"testing"
	"github.com/inconshreveable/log15"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/mock"
	"fmt"
	"time"
	"math/rand"
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"github.com/cfchou/porter/service"
	"github.com/afex/hystrix-go/hystrix"
)

func TestMain(m *testing.M) {
	flag.Parse()
	//h := log15.LvlFilterHandler(log15.LvlDebug, log15.CallerFuncHandler(log15.StdoutHandler))
	h := log15.LvlFilterHandler(log15.LvlDebug,
		log15.MultiHandler(
			log15.StdoutHandler,
			log15.Must.FileHandler("./test.log", log15.LogfmtFormat())))
	service.Log.SetHandler(h)
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

func (m *MockSpec) ToReceiveMessageInput() (*sqs.ReceiveMessageInput, error) {
	args := m.Called()
	return args.Get(0).(*sqs.ReceiveMessageInput), args.Error(1)
}

func fakeSqsUpStreamConf() *SqsUpStreamConf{
	return &SqsUpStreamConf{
		MaxWaitingMessages: 10,
		RequestVolumeThreshold: 1,
		ErrorPercentThreshold: 1,
		SleepWindow: 1000,
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

func _TestSqsUpStream_WaitMessage_One(t *testing.T) {
	fake_conf := fakeSqsUpStreamConf()
	fake_input := sqs.ReceiveMessageInput{}
	fake_output := fakeReceiveMessageOutput(1)

	mspec := &MockSpec{}
	mspec.On("ToReceiveMessageInput").Return(&fake_input, nil)

	// Calls except for the first one will be blocked
	done := make(chan *struct{}, 1)
	mc := &MockClient{}
	mc.On("ReceiveMessage", &fake_input).Run(func(args mock.Arguments) {
		done <- &struct{}{}
	}).Return(fake_output, nil)

	// Must give a different name across all tests to prevent clashes of the circuit breakers.
	name := fmt.Sprintf("Wait_One_%d", rand.Int63n(time.Now().Unix()))
	up, err := NewSqsUpStream(name, *fake_conf, mc, mspec)
	assert.Nil(t, err)
	go up.Run()
	msg, err := up.WaitMessage(0)
	assert.Nil(t, err)
	sqs_msg, ok := msg.(*sqs.Message)
	assert.True(t, ok)
	assert.EqualValues(t, *sqs_msg, *fake_output.Messages[0])
}

func _TestSqsUpStream_WaitMessage_One_Multi(t *testing.T) {
	count := 5
	fake_conf := fakeSqsUpStreamConf()
	fake_input := sqs.ReceiveMessageInput{}
	fake_output := fakeReceiveMessageOutput(1)

	mspec := &MockSpec{}
	mspec.On("ToReceiveMessageInput").Return(&fake_input, nil)

	// Calls after $count times will be blocked
	done := make(chan *struct{}, count)
	mc := &MockClient{}
	mc.On("ReceiveMessage", &fake_input).Run(func(args mock.Arguments) {
		done <- &struct{}{}
	}).Return(fake_output, nil)

	// Must give a different name across all tests to prevent clashes of the circuit breakers.
	name := fmt.Sprintf("Wait_One_Multi_%d", rand.Int63n(time.Now().Unix()))
	up, err := NewSqsUpStream(name, *fake_conf, mc, mspec)
	assert.Nil(t, err)
	go up.Run()

	for i := 1; i <= count; i++ {
		Log.Info("[Test]", "service", name, "num", i, "count", count)
		msg, err := up.WaitMessage(0)
		assert.Nil(t, err)
		sqs_msg, ok := msg.(*sqs.Message)
		assert.True(t, ok)
		assert.EqualValues(t, *sqs_msg, *fake_output.Messages[0])
	}
}

func _TestSqsUpStream_WaitMessage_Many(t *testing.T) {
	many := 10
	fake_conf := fakeSqsUpStreamConf()
	fake_input := sqs.ReceiveMessageInput{}
	fake_output := fakeReceiveMessageOutput(many)

	mspec := &MockSpec{}
	mspec.On("ToReceiveMessageInput").Return(&fake_input, nil)

	done := make(chan *struct{}, 1)
	mc := &MockClient{}
	mc.On("ReceiveMessage", &fake_input).Run(func(args mock.Arguments) {
		done <- &struct{}{}
	}).Return(fake_output, nil)

	name := fmt.Sprintf("Wait_Many_%d", rand.Int63n(time.Now().Unix()))
	up, err := NewSqsUpStream(name, *fake_conf, mc, mspec)
	assert.Nil(t, err)
	go up.Run()

	for i := 0; i < many; i++ {
		msg, err := up.WaitMessage(0)
		assert.Nil(t, err)
		sqs_msg, ok := msg.(*sqs.Message)
		assert.True(t, ok)
		assert.EqualValues(t, *sqs_msg, *fake_output.Messages[i])
	}
}

func _TestSqsUpStream_WaitMessage_Many_Multi(t *testing.T) {
	count := 5
	many := 5
	fake_conf := fakeSqsUpStreamConf()
	fake_input := sqs.ReceiveMessageInput{}
	fake_output := fakeReceiveMessageOutput(many)

	mspec := &MockSpec{}
	mspec.On("ToReceiveMessageInput").Return(&fake_input, nil)

	done := make(chan *struct{}, count)
	mc := &MockClient{}
	mc.On("ReceiveMessage", &fake_input).Run(func(args mock.Arguments) {
		done <- &struct{}{}
	}).Return(fake_output, nil)

	name := fmt.Sprintf("Wait_Many_Multi_%d", rand.Int63n(time.Now().Unix()))
	up, err := NewSqsUpStream(name, *fake_conf, mc, mspec)
	assert.Nil(t, err)
	go up.Run()

	for i := 1; i <= count; i++ {
		Log.Info("[Test]", "service", name, "num", i, "count", count)
		for j := 0; j < many; j++ {
			msg, err := up.WaitMessage(0)
			assert.Nil(t, err)
			sqs_msg, ok := msg.(*sqs.Message)
			assert.True(t, ok)
			assert.EqualValues(t, *sqs_msg, *fake_output.Messages[j])
		}
	}
}

func _TestSqsDownStream_Run(t *testing.T) {
	count := 5
	many := 5
	fake_conf := fakeSqsUpStreamConf()
	fake_input := sqs.ReceiveMessageInput{}
	fake_output := fakeReceiveMessageOutput(many)

	mspec := &MockSpec{}
	mspec.On("ToReceiveMessageInput").Return(&fake_input, nil)

	up_done := make(chan *struct{}, count)
	mc := &MockClient{}
	recv_times := 0
	mc.On("ReceiveMessage", &fake_input).Run(func(args mock.Arguments) {
		// blocked here when recv_times == count
		up_done <- &struct{}{}
		recv_times++
		Log.Info("[Test] Up message", "recv_times", recv_times)
	}).Return(fake_output, nil)

	tm := rand.Int63n(time.Now().Unix())
	up, err := NewSqsUpStream(fmt.Sprintf("Up_%d", tm), *fake_conf, mc, mspec)
	assert.Nil(t, err)
	go up.Run()

	fake_down_conf := service.DefaultDownStreamConf{
		// Allow the handler to run slowly(up to 10 secs)
		Timeout: 10000,
		ConcurrentHandlers:3,
		ErrorPercentThreshold:1,
		RequestVolumeThreshold:1,
		SleepWindow:1000,
	}
	down := service.NewDefaultDownStream(fmt.Sprintf("Down_%d", tm), fake_down_conf)
	total := 0
	down_done := make(chan *struct{})
	go down.Run(up, func(msg interface{}) error {
		total++
		Log.Info("[Test] Down message", "total", total)
		if total == count * many {
			down_done <- &struct{}{}
		}
		return nil
	})
	<- down_done
}

func TestSqsUpStream_Run_Back_Pressured(t *testing.T) {
	count := 5
	many := 5
	fake_conf := &SqsUpStreamConf{
		MaxWaitingMessages: 3,
		RequestVolumeThreshold: 1,
		ErrorPercentThreshold: 1,
		SleepWindow: 1000,
	}
	fake_input := sqs.ReceiveMessageInput{}
	fake_output := fakeReceiveMessageOutput(many)

	mspec := &MockSpec{}
	mspec.On("ToReceiveMessageInput").Return(&fake_input, nil)

	mc := &MockClient{}
	recv_times := 0
	mc.On("ReceiveMessage", &fake_input).Run(func(args mock.Arguments) {
		recv_times++
		Log.Info("[Test] Up message", "recv_times", recv_times)
	}).Return(fake_output, nil)

	tm := rand.Int63n(time.Now().Unix())
	up, err := NewSqsUpStream(fmt.Sprintf("Up_%d", tm), *fake_conf, mc, mspec)
	assert.Nil(t, err)
	fake_down_conf := service.DefaultDownStreamConf{
		// Timeout doesn't interrupt handler. It only contributes an
		// ErrTimeout the metrics in the circuit breaker.
		Timeout: 10000,
		ConcurrentHandlers: 2,
		ErrorPercentThreshold: 1,
		RequestVolumeThreshold: 1,
		SleepWindow: 1000, // circuit turns half-opened from opened
	}
	backOff := NewBackOffImpl(1000, 5, 1000)
	down := service.NewDefaultDownStream(fmt.Sprintf("Down_%d", tm), fake_down_conf)
	up.SetBackPressure(down, backOff)
	go up.Run()
	var total int32 = 0
	down_done := make(chan *struct{})
	go down.Run(up, func(msg interface{}) error {
		v := atomic.AddInt32(&total, 1)
		if v < 15 {
			time.Sleep(time.Duration(1)*time.Second)
			return fmt.Errorf("** Err %d", v)
		}
		Log.Info("[Test] Down message", "total", v)
		if v == int32(count * many) {
			down_done <- &struct{}{}
		}
		return nil
	})
	<- down_done
}
