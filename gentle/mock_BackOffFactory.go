package gentle

import "github.com/stretchr/testify/mock"

// Generated by github.com/vektra/mockery

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
