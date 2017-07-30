package gentle

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"strconv"
	"testing"
)

func TestAppendHandlersStream(t *testing.T) {
	// Append multiple Handlers to a Stream. Handlers should run in sequence.
	mstream := &MockStream{}
	mhandlers := []Handler{&MockHandler{}, &MockHandler{}, &MockHandler{}}
	stream := AppendHandlersStream(mstream, mhandlers...)
	id := 0
	mm := &fakeMsg{id: strconv.Itoa(id)}
	mstream.On("Get").Return(mm, nil)

	for _, mhandler := range mhandlers {
		mhandler.(*MockHandler).On("Handle", mock.Anything).Return(
			func(msg Message) Message {
				i, err := strconv.Atoi(msg.ID())
				assert.NoError(t, err)
				return &fakeMsg{id: strconv.Itoa(i + 1)}
			}, nil)
	}

	msg, err := stream.Get()
	assert.NoError(t, err)
	assert.Equal(t, strconv.Itoa(id+len(mhandlers)), msg.ID())
}

func TestAppendHandlersStream_SimpleHandler(t *testing.T) {
	// AppendHandlerStream accepts SimpleHandlers and functions converted to
	// SimpleHandler
	mstream := &MockStream{}
	id := "0"
	id2 := "1"
	id3 := "2"

	var h1 SimpleHandler = func(msg Message) (Message, error) {
		assert.Equal(t, id, msg.ID())
		return &fakeMsg{id2}, nil
	}

	h2 := func(msg Message) (Message, error) {
		assert.Equal(t, id2, msg.ID())
		return &fakeMsg{id3}, nil
	}

	stream := AppendHandlersStream(mstream, h1, (SimpleHandler)(h2))

	mm := &fakeMsg{id}
	mstream.On("Get").Return(mm, nil)

	msg, err := stream.Get()
	assert.NoError(t, err)
	assert.Equal(t, id3, msg.ID())
}

func TestAppendHandlersStream_UpstreamFail(t *testing.T) {
	// A failing Stream would not trigger subsequent Handlers
	mstream := &MockStream{}
	mhandler := &MockHandler{}
	stream := AppendHandlersStream(mstream, mhandler)

	fakeErr := errors.New("stream failed")
	mstream.On("Get").Return((*fakeMsg)(nil), fakeErr)
	mhandler.On("Handle", mock.Anything).Run(
		func(arguments mock.Arguments) {
			assert.FailNow(t, "shouldn't run")
		})

	msg, err := stream.Get()
	assert.Nil(t, msg)
	assert.EqualError(t, err, fakeErr.Error())
}

func TestAppendHandlersStream_FallThrough(t *testing.T) {
	// Append multiple Handlers to a Stream. Handlers should run in sequence.
	// One failing Handler would bypass all subsequent Handlers.
	mstream := &MockStream{}
	mhandlers := []Handler{&MockHandler{}, &MockHandler{}, &MockHandler{}}
	stream := AppendHandlersStream(mstream, mhandlers...)
	id := 0
	mm := &fakeMsg{id: strconv.Itoa(id)}
	mstream.On("Get").Return(mm, nil)

	mhandlers[0].(*MockHandler).On("Handle", mock.Anything).Return(
		func(msg Message) Message {
			id++
			i, err := strconv.Atoi(msg.ID())
			assert.NoError(t, err)
			return &fakeMsg{id: strconv.Itoa(i + 1)}
		}, nil)

	fakeErr := errors.New("handler failed")
	mhandlers[1].(*MockHandler).On("Handle", mock.Anything).
		Return((*fakeMsg)(nil), fakeErr)

	mhandlers[2].(*MockHandler).On("Handle", mock.Anything).Run(
		func(arguments mock.Arguments) {
			assert.FailNow(t, "shouldn't run")
		})

	msg, err := stream.Get()
	assert.Nil(t, msg)
	assert.EqualError(t, err, fakeErr.Error())
	assert.Equal(t, 1, id)
}

func TestAppendFallbacksStream(t *testing.T) {
	fakeErr := errors.New("stream error")
	fallbackErrors := []error{
		errors.New("Fallback error 0"),
		errors.New("Fallback error 1"),
		errors.New("Fallback error 2"),
	}
	mstream := &MockStream{}
	fallbacks := []StreamFallback{
		func(err error) (Message, error) {
			assert.EqualError(t, err, fakeErr.Error())
			return nil, fallbackErrors[0]
		},
		func(err error) (Message, error) {
			assert.EqualError(t, err, fallbackErrors[0].Error())
			return nil, fallbackErrors[1]
		},
		func(err error) (Message, error) {
			assert.EqualError(t, err, fallbackErrors[1].Error())
			return nil, fallbackErrors[2]
		},
	}

	fstream := AppendFallbacksStream(mstream, fallbacks...)

	mstream.On("Get").Return((*fakeMsg)(nil), fakeErr)

	msg, err := fstream.Get()
	assert.Nil(t, msg)
	assert.EqualError(t, err, fallbackErrors[2].Error())
}

func TestAppendFallbacksStream_UpstreamSucceed(t *testing.T) {
	mstream := &MockStream{}
	fallback := func(err error) (Message, error) {
		assert.FailNow(t, "shouldn't run")
		return nil, nil
	}

	fstream := AppendFallbacksStream(mstream, fallback)

	mm := &fakeMsg{"0"}
	mstream.On("Get").Return(mm, nil)

	msg, err := fstream.Get()
	assert.NoError(t, err)
	assert.Equal(t, mm.ID(), msg.ID())
}

func TestAppendFallbacksStream_FallThrough(t *testing.T) {
	fakeErr := errors.New("stream error")
	fallbackErr := errors.New("fallback error")
	mm := &fakeMsg{"123"}
	mstream := &MockStream{}
	fallbacks := []StreamFallback{
		func(err error) (Message, error) {
			assert.EqualError(t, err, fakeErr.Error())
			return nil, fallbackErr
		},
		func(err error) (Message, error) {
			assert.EqualError(t, err, fallbackErr.Error())
			return mm, nil
		},
		func(err error) (Message, error) {
			assert.FailNow(t, "shouldn't run")
			return nil, nil
		},
	}

	fstream := AppendFallbacksStream(mstream, fallbacks...)

	mstream.On("Get").Return((*fakeMsg)(nil), fakeErr)

	msg, err := fstream.Get()
	assert.NoError(t, err)
	assert.Equal(t, mm.ID(), msg.ID())
}

func TestAppendHandlersHandler(t *testing.T) {
	// Append multiple Handlers to a Handler. Handlers should run in sequence.
	mhandler := &MockHandler{}
	mhandlers := []Handler{&MockHandler{}, &MockHandler{}, &MockHandler{}}
	handler := AppendHandlersHandler(mhandler, mhandlers...)
	id := 0
	mm := &fakeMsg{id: strconv.Itoa(id)}
	mhandler.On("Handle", mock.Anything).Return(mm, nil)

	for _, h := range mhandlers {
		h.(*MockHandler).On("Handle", mock.Anything).Return(
			func(msg Message) Message {
				i, err := strconv.Atoi(msg.ID())
				assert.NoError(t, err)
				return &fakeMsg{id: strconv.Itoa(i + 1)}
			}, nil)
	}

	msg, err := handler.Handle(mm)
	assert.NoError(t, err)
	assert.Equal(t, strconv.Itoa(id+len(mhandlers)), msg.ID())
}

func TestAppendHandlersHandler_SimpleHandler(t *testing.T) {
	// AppendHandlerHandler accepts SimpleHandlers and functions converted to
	// SimpleHandler
	mhandler := &MockHandler{}
	id := "0"
	id2 := "1"
	id3 := "2"

	var h1 SimpleHandler = func(msg Message) (Message, error) {
		assert.Equal(t, id, msg.ID())
		return &fakeMsg{id2}, nil
	}

	h2 := func(msg Message) (Message, error) {
		assert.Equal(t, id2, msg.ID())
		return &fakeMsg{id3}, nil
	}

	handler := AppendHandlersHandler(mhandler, h1, (SimpleHandler)(h2))

	mm := &fakeMsg{id}
	mhandler.On("Handle", mock.Anything).Return(mm, nil)

	msg, err := handler.Handle(mm)
	assert.NoError(t, err)
	assert.Equal(t, id3, msg.ID())
}

func TestAppendHandlersHandler_FirstHandlerFail(t *testing.T) {
	// A failing Handler would not trigger subsequent Handlers
	mhandler := &MockHandler{}
	mhandler2 := &MockHandler{}
	handler := AppendHandlersHandler(mhandler, mhandler2)

	fakeErr := errors.New("handler failed")
	mhandler.On("Handle", mock.Anything).Return((*fakeMsg)(nil), fakeErr)
	mhandler2.On("Handle", mock.Anything).Run(
		func(arguments mock.Arguments) {
			assert.FailNow(t, "shouldn't run")
		})

	msg, err := handler.Handle(&fakeMsg{"123"})
	assert.Nil(t, msg)
	assert.EqualError(t, err, fakeErr.Error())
}

func TestAppendHandlersHandler_FallThrough(t *testing.T) {
	// Append multiple Handlers to a Handler. Handlers should run in sequence.
	// One failing Handler would bypass all subsequent Handlers.
	mhandler := &MockHandler{}
	mhandlers := []Handler{&MockHandler{}, &MockHandler{}, &MockHandler{}}
	handler := AppendHandlersHandler(mhandler, mhandlers...)
	id := 0
	mm := &fakeMsg{id: strconv.Itoa(id)}
	mhandler.On("Handle", mock.Anything).Return(mm, nil)

	mhandlers[0].(*MockHandler).On("Handle", mock.Anything).Return(
		func(msg Message) Message {
			id++
			i, err := strconv.Atoi(msg.ID())
			assert.NoError(t, err)
			return &fakeMsg{id: strconv.Itoa(i + 1)}
		}, nil)

	fakeErr := errors.New("handler failed")
	mhandlers[1].(*MockHandler).On("Handle", mock.Anything).
		Return((*fakeMsg)(nil), fakeErr)

	mhandlers[2].(*MockHandler).On("Handle", mock.Anything).Run(
		func(arguments mock.Arguments) {
			assert.FailNow(t, "shouldn't run")
		})

	msg, err := handler.Handle(mm)
	assert.Nil(t, msg)
	assert.EqualError(t, err, fakeErr.Error())
	assert.Equal(t, 1, id)
}

func TestAppendFallbacksHandler(t *testing.T) {
	fakeErr := errors.New("handler error")
	fallbackErrors := []error{
		errors.New("Fallback error 0"),
		errors.New("Fallback error 1"),
		errors.New("Fallback error 2"),
	}
	mhandler := &MockHandler{}
	mm := &fakeMsg{"123"}
	fallbacks := []HandlerFallback{
		func(msg Message, err error) (Message, error) {
			assert.Equal(t, mm.ID(), msg.ID())
			assert.EqualError(t, err, fakeErr.Error())
			return nil, fallbackErrors[0]
		},
		func(msg Message, err error) (Message, error) {
			assert.Equal(t, mm.ID(), msg.ID())
			assert.EqualError(t, err, fallbackErrors[0].Error())
			return nil, fallbackErrors[1]
		},
		func(msg Message, err error) (Message, error) {
			assert.Equal(t, mm.ID(), msg.ID())
			assert.EqualError(t, err, fallbackErrors[1].Error())
			return nil, fallbackErrors[2]
		},
	}

	fhandler := AppendFallbacksHandler(mhandler, fallbacks...)

	mhandler.On("Handle", mock.Anything).Return((*fakeMsg)(nil), fakeErr)

	msg, err := fhandler.Handle(mm)
	assert.Nil(t, msg)
	assert.EqualError(t, err, fallbackErrors[2].Error())
}

func TestAppendFallbacksStream_FirstHandlerSucceed(t *testing.T) {
	mhandler := &MockHandler{}
	fallback := func(msg Message, err error) (Message, error) {
		assert.FailNow(t, "shouldn't run")
		return nil, nil
	}

	fstream := AppendFallbacksHandler(mhandler, fallback)

	mm := &fakeMsg{"0"}
	mhandler.On("Handle", mock.Anything).Return(mm, nil)

	msg, err := fstream.Handle(mm)
	assert.NoError(t, err)
	assert.Equal(t, mm.ID(), msg.ID())
}

func TestAppendFallbacksHandler_FallThrough(t *testing.T) {
	fakeErr := errors.New("stream error")
	fallbackErr := errors.New("fallback error")
	mm := &fakeMsg{"123"}
	mhandler := &MockHandler{}
	fallbacks := []HandlerFallback{
		func(msg Message, err error) (Message, error) {
			assert.Equal(t, mm.ID(), msg.ID())
			assert.EqualError(t, err, fakeErr.Error())
			return nil, fallbackErr
		},
		func(msg Message, err error) (Message, error) {
			assert.Equal(t, mm.ID(), msg.ID())
			assert.EqualError(t, err, fallbackErr.Error())
			return mm, nil
		},
		func(msg Message, err error) (Message, error) {
			assert.FailNow(t, "shouldn't run")
			return nil, nil
		},
	}

	fhandler := AppendFallbacksHandler(mhandler, fallbacks...)

	mhandler.On("Handle", mock.Anything).Return((*fakeMsg)(nil), fakeErr)

	msg, err := fhandler.Handle(mm)
	assert.NoError(t, err)
	assert.Equal(t, mm.ID(), msg.ID())
}
