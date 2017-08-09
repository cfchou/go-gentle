package gentle

import "github.com/stretchr/testify/mock"

import "time"

// Generated by github.com/vektra/mockery

// MockStream mocks Stream interface
type MockStream struct {
	mock.Mock
}

// Get provides a mock function with given fields:
func (_m *MockStream) Get() (Message, error) {
	ret := _m.Called()

	var r0 Message
	if rf, ok := ret.Get(0).(func() Message); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(Message)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockHandler mocks Handler interface
type MockHandler struct {
	mock.Mock
}

// Handle provides a mock function with given fields: msg
func (_m *MockHandler) Handle(msg Message) (Message, error) {
	ret := _m.Called(msg)

	var r0 Message
	if rf, ok := ret.Get(0).(func(Message) Message); ok {
		r0 = rf(msg)
	} else {
		r0 = ret.Get(0).(Message)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(Message) error); ok {
		r1 = rf(msg)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockBackOff mocks BackOff interface
type MockBackOff struct {
	mock.Mock
}

// Next provides a mock function with given fields:
func (_m *MockBackOff) Next() time.Duration {
	ret := _m.Called()

	var r0 time.Duration
	if rf, ok := ret.Get(0).(func() time.Duration); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(time.Duration)
	}

	return r0
}

// MockBackOffFactory mocks BackOffFactory interface
type MockBackOffFactory struct {
	mock.Mock
}

// NewBackOff provides a mock function with given fields:
func (_m *MockBackOffFactory) NewBackOff() BackOff {
	ret := _m.Called()

	var r0 BackOff
	if rf, ok := ret.Get(0).(func() BackOff); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(BackOff)
	}

	return r0
}
