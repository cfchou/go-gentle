// vim:fileencoding=utf-8
package service

import (
	"flag"
	"testing"
	"github.com/inconshreveable/log15"
	"time"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/assert"
	"sync"
	"github.com/afex/hystrix-go/hystrix"
)

var log = Log.New()

func TestMain(m *testing.M) {
	flag.Parse()
	//h := log15.LvlFilterHandler(log15.LvlDebug, log15.CallerFuncHandler(log15.StdoutHandler))
	h := log15.LvlFilterHandler(log15.LvlDebug,
		log15.MultiHandler(
			log15.StdoutHandler,
			log15.Must.FileHandler("./test.log", log15.LogfmtFormat())))
	log.SetHandler(h)
	m.Run()
}


type mockSender struct {
	mock.Mock
	log log15.Logger

	msg Message
	timeout time.Duration
}

func (m *mockSender) SendMessage(msg Message, timeout time.Duration) (Message, error) {
	// Use m.msg and m.timeout because we don't care about the arguments
	// passed in.
	args := m.Called(m.msg, m.timeout)
	return args.Get(0).(Message), args.Error(1)
}

func (m *mockSender) Logger() log15.Logger {
	return m.log
}

type mockSender2 struct {
	mock.Mock
	log log15.Logger
}

func (m *mockSender2) SendMessage(msg Message, timeout time.Duration) (Message, error) {
	args := m.Called(msg, timeout)
	m.log.Debug("[Test] mock.SendMessage()", "msg", msg.Id(),
		"when", time.Now())
	return args.Get(0).(Message), args.Error(1)
}

func (m *mockSender2) Logger() log15.Logger {
	return m.log
}

type TestMsg struct {
	Message
	timeout time.Duration
}


func newTestMsgs(count int) []TestMsg {
	var cmds []TestMsg
	for i := 1; i <= count; i++ {
		cmds = append(cmds, TestMsg{
			Message: NewMsg(),
			timeout: 0,
		})
	}
	return cmds
}

func TestRateLimitedSender_SendMessage(t *testing.T) {
	request_interval := 300
	count := 10
	ms := &mockSender2{
		log: log.New(),
	}

	var cmds = newTestMsgs(count)
	for _, v := range cmds {
		ms.On("SendMessage", v, v.timeout).Return(v, nil)
	}

	rls := NewRateLimitedSender("test", ms,
		NewTokenBucketRateLimit(
			TokenBucketRateLimitConf{
				RequestsInterval: request_interval,
				MaxRequestsBurst: 1,
			}))

	begin := time.Now()
	// count - 1 since the token-bucket is fully filled at the beginning.
	end_expected := begin.Add(IntToMillis((count - 1)*request_interval))
	for _, v := range cmds {
		u, _ := rls.SendMessage(v, v.timeout)
		assert.EqualValues(t, u, v)
	}
	end := time.Now()
	log.Debug("[Test] status", "begin", begin, "end", end, "end_expected", end_expected)
	assert.False(t, end.Before(end_expected))
}

func TestRateLimitedSender_SendMessage_Concurrent(t *testing.T) {
	request_interval := 300
	count := 10
	ms := &mockSender2{
		log: log.New(),
	}

	var cmds = newTestMsgs(count)
	for _, v := range cmds {
		ms.On("SendMessage", v, v.timeout).Return(v, nil)
	}

	rls := NewRateLimitedSender("test", ms,
		NewTokenBucketRateLimit(
			TokenBucketRateLimitConf{
				RequestsInterval: request_interval,
				MaxRequestsBurst: 1,
			}))

	var wg sync.WaitGroup
	wg.Add(count)
	begin := time.Now()
	// count - 1 since the token-bucket is fully filled at the beginning.
	end_expected := begin.Add(IntToMillis((count - 1)*request_interval))
	for _, v := range cmds {
		go func(v TestMsg) {
			u, _ := rls.SendMessage(v, v.timeout)
			assert.EqualValues(t, u, v)
			wg.Done()
		}(v)
	}
	wg.Wait()
	end := time.Now()
	log.Debug("[Test] status", "begin", begin, "end", end, "end_expected", end_expected)
	assert.False(t, end.Before(end_expected))
}

func TestCircuitBreakerSender_SendMessage(t *testing.T) {
	count := 10
	ms := &mockSender2{
		log: log.New(),
	}

	var cmds = newTestMsgs(count)
	for _, v := range cmds {
		ms.On("SendMessage", v, v.timeout).Return(v, nil)
	}

	cbs := NewCircuitBreakerSender("test", ms, hystrix.CommandConfig{
		Timeout:                hystrix.DefaultTimeout,
		MaxConcurrentRequests:  hystrix.DefaultMaxConcurrent,
		RequestVolumeThreshold: hystrix.DefaultVolumeThreshold,
		SleepWindow:            hystrix.DefaultSleepWindow,
		ErrorPercentThreshold:  hystrix.DefaultErrorPercentThreshold,
	})

	for _, v := range cmds {
		u, _ := cbs.SendMessage(v, v.timeout)
		assert.EqualValues(t, u, v)
	}
}

// Mixin1:
// CircuitBreakerSender(
// 	RateLimitedSender(
// 		mockSender)))
// Requests blocked by rate limiter would pass circuit breaker's timeout
// causing circuit to be opened.
func TestSender_Mixin1(t *testing.T) {

	request_interval := 300
	count := 6
	ms := &mockSender2{
		log: log.New(),
	}

	var cmds = newTestMsgs(count)
	for _, v := range cmds {
		ms.On("SendMessage", v, v.timeout).Return(v, nil)
	}

	rls := NewRateLimitedSender("test", ms,
		NewTokenBucketRateLimit(
			TokenBucketRateLimitConf{
				RequestsInterval: request_interval,
				MaxRequestsBurst: 1,
			}))

	cbs := NewCircuitBreakerSender("test", rls, hystrix.CommandConfig{
		Timeout:                hystrix.DefaultTimeout,
		MaxConcurrentRequests:  hystrix.DefaultMaxConcurrent,
		RequestVolumeThreshold: hystrix.DefaultVolumeThreshold,
		SleepWindow:            hystrix.DefaultSleepWindow,
		ErrorPercentThreshold:  hystrix.DefaultErrorPercentThreshold,
	})

	var wg sync.WaitGroup
	wg.Add(count)
	begin := time.Now()
	// count - 1 since the token-bucket is fully filled at the beginning.
	end_expected := begin.Add(IntToMillis((count - 1)*request_interval))
	for _, v := range cmds {
		go func(v TestMsg) {
			u, _ := cbs.SendMessage(v, v.timeout)
			assert.EqualValues(t, u, v)
			wg.Done()
		}(v)
	}
	wg.Wait()
	end := time.Now()
	log.Debug("[Test] status", "begin", begin, "end", end, "end_expected", end_expected)
	assert.False(t, end.Before(end_expected))
}

// Mixin2:
// RateLimitedSender(
// 	CircuitBreakerSender(
// 		mockSender)))
func TestSender_Mixin2(t *testing.T) {

	request_interval := 300
	count := 10
	ms := &mockSender2{
		log: log.New(),
	}

	var cmds = newTestMsgs(count)
	for _, v := range cmds {
		ms.On("SendMessage", v, v.timeout).Return(v, nil)
	}

	cbs := NewCircuitBreakerSender("test", ms, hystrix.CommandConfig{
		Timeout:                hystrix.DefaultTimeout,
		MaxConcurrentRequests:  hystrix.DefaultMaxConcurrent,
		RequestVolumeThreshold: hystrix.DefaultVolumeThreshold,
		SleepWindow:            hystrix.DefaultSleepWindow,
		ErrorPercentThreshold:  hystrix.DefaultErrorPercentThreshold,
	})

	rls := NewRateLimitedSender("test", cbs,
		NewTokenBucketRateLimit(
			TokenBucketRateLimitConf{
				RequestsInterval: request_interval,
				MaxRequestsBurst: 1,
			}))


	var wg sync.WaitGroup
	wg.Add(count)
	begin := time.Now()
	// count - 1 since the token-bucket is fully filled at the beginning.
	end_expected := begin.Add(IntToMillis((count - 1)*request_interval))
	for _, v := range cmds {
		go func(v TestMsg) {
			u, _ := rls.SendMessage(v, v.timeout)
			assert.EqualValues(t, u, v)
			wg.Done()
		}(v)
	}
	wg.Wait()
	end := time.Now()
	log.Debug("[Test] status", "begin", begin, "end", end, "end_expected", end_expected)
	assert.False(t, end.Before(end_expected))
}
