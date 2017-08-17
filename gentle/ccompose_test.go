package gentle

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"strconv"
	"testing"
	"testing/quick"
	"time"
)

func TestAppendHandlersCStream(t *testing.T) {
	// Append multiple Handlers to a Stream. Handlers should run in sequence.
	mstream := &MockCStream{}
	mhandlers := []CHandler{&MockCHandler{}, &MockCHandler{}, &MockCHandler{}}
	stream := AppendHandlersCStream(mstream, mhandlers...)
	id := 0
	mm := &fakeMsg{id: strconv.Itoa(id)}
	mstream.On("Get", mock.Anything).Return(mm, nil)

	for _, mhandler := range mhandlers {
		mhandler.(*MockCHandler).On("Handle", mock.Anything, mock.Anything).Return(
			func(_ context.Context, msg Message) Message {
				i, _ := strconv.Atoi(msg.ID())
				return &fakeMsg{id: strconv.Itoa(i + 1)}
			}, nil)
	}

	msg, err := stream.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, strconv.Itoa(id+len(mhandlers)), msg.ID())
}

func TestAppendHandlersCStream_Timeout(t *testing.T) {
	// Timeout happens in appended stream and handlers
	suspend := 100 * time.Millisecond
	run := func(timeoutMs int) bool {
		mstream := &MockCStream{}
		mhandlers := []CHandler{&MockCHandler{}, &MockCHandler{}, &MockCHandler{}}
		stream := AppendHandlersCStream(mstream, mhandlers...)
		id := 0
		mm := &fakeMsg{id: strconv.Itoa(id)}
		var maybeErr error
		mstream.On("Get", mock.Anything).Return(func(ctx context.Context) Message {
			tm := time.NewTimer(suspend)
			defer tm.Stop()
			select {
			case <-ctx.Done():
				maybeErr = ctx.Err()
				return nil
			case <-tm.C:
				return mm
			}
		}, func(_ context.Context) error {
			return maybeErr
		})

		for _, mhandler := range mhandlers {
			var maybeErr error
			mhandler.(*MockCHandler).On("Handle", mock.Anything, mock.Anything).Return(
				func(ctx context.Context, msg Message) Message {
					tm := time.NewTimer(suspend)
					defer tm.Stop()
					select {
					case <-ctx.Done():
						maybeErr = ctx.Err()
						return nil
					case <-tm.C:
						i, _ := strconv.Atoi(msg.ID())
						return &fakeMsg{id: strconv.Itoa(i + 1)}
					}
				}, func(_ context.Context, _ Message) error {
					return maybeErr
				})
		}

		ctx, cancel := context.WithTimeout(context.Background(),
			time.Duration(timeoutMs)*time.Millisecond)
		defer cancel()
		_, err := stream.Get(ctx)
		return err == context.DeadlineExceeded
	}
	config := &quick.Config{
		// [1ms, 3*suspend)
		Values: genBoundInt(1, 300),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestAppendHandlersCStream_SimpleHandler(t *testing.T) {
	// AppendHandlerStream accepts SimpleHandlers and functions converted to
	// SimpleHandler
	mstream := &MockCStream{}
	id := "0"
	id2 := "1"
	id3 := "2"

	var h1 SimpleCHandler = func(_ context.Context, msg Message) (Message, error) {
		assert.Equal(t, id, msg.ID())
		return &fakeMsg{id2}, nil
	}

	h2 := func(_ context.Context, msg Message) (Message, error) {
		assert.Equal(t, id2, msg.ID())
		return &fakeMsg{id3}, nil
	}

	stream := AppendHandlersCStream(mstream, h1, (SimpleCHandler)(h2))

	mm := &fakeMsg{id}
	mstream.On("Get", mock.Anything).Return(mm, nil)

	msg, err := stream.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, id3, msg.ID())
}

func TestAppendHandlersCStream_UpstreamFail(t *testing.T) {
	// A failing Stream would not trigger subsequent Handlers
	mstream := &MockCStream{}
	mhandler := &MockCHandler{}
	stream := AppendHandlersCStream(mstream, mhandler)

	fakeErr := errors.New("stream failed")
	mstream.On("Get", mock.Anything).Return((*fakeMsg)(nil), fakeErr)
	mhandler.On("Handle", mock.Anything, mock.Anything).Return(
		func(_ context.Context, _ Message) Message {
			assert.FailNow(t, "shouldn't run")
			return nil
		}, nil)

	msg, err := stream.Get(context.Background())
	assert.Nil(t, msg)
	assert.EqualError(t, err, fakeErr.Error())
}

func TestAppendHandlersCStream_FallThrough(t *testing.T) {
	// Append multiple Handlers to a Stream. Handlers should run in sequence.
	// One failing Handler would bypass all subsequent Handlers.
	mstream := &MockCStream{}
	mhandlers := []CHandler{&MockCHandler{}, &MockCHandler{}, &MockCHandler{}}
	stream := AppendHandlersCStream(mstream, mhandlers...)
	id := 0
	mm := &fakeMsg{id: strconv.Itoa(id)}
	mstream.On("Get", mock.Anything).Return(mm, nil)

	mhandlers[0].(*MockCHandler).On("Handle", mock.Anything, mock.Anything).
		Return(func(_ context.Context, msg Message) Message {
			id++
			i, _ := strconv.Atoi(msg.ID())
			return &fakeMsg{id: strconv.Itoa(i + 1)}
		}, nil)

	fakeErr := errors.New("handler failed")
	mhandlers[1].(*MockCHandler).On("Handle", mock.Anything, mock.Anything).
		Return((*fakeMsg)(nil), fakeErr)

	mhandlers[2].(*MockCHandler).On("Handle", mock.Anything, mock.Anything).
		Return(func(_ context.Context, _ Message) Message {
			assert.FailNow(t, "shouldn't run")
			return nil
		}, nil)

	msg, err := stream.Get(context.Background())
	assert.Nil(t, msg)
	assert.EqualError(t, err, fakeErr.Error())
	assert.Equal(t, 1, id)
}

func TestAppendFallbacksCStream(t *testing.T) {
	fakeErr := errors.New("stream error")
	fallbackErrors := []error{
		errors.New("Fallback error 0"),
		errors.New("Fallback error 1"),
		errors.New("Fallback error 2"),
	}
	mstream := &MockCStream{}
	fallbacks := []CStreamFallback{
		func(_ context.Context, err error) (Message, error) {
			assert.EqualError(t, err, fakeErr.Error())
			return nil, fallbackErrors[0]
		},
		func(_ context.Context, err error) (Message, error) {
			assert.EqualError(t, err, fallbackErrors[0].Error())
			return nil, fallbackErrors[1]
		},
		func(_ context.Context, err error) (Message, error) {
			assert.EqualError(t, err, fallbackErrors[1].Error())
			return nil, fallbackErrors[2]
		},
	}

	fstream := AppendFallbacksCStream(mstream, fallbacks...)

	mstream.On("Get", mock.Anything).Return((*fakeMsg)(nil), fakeErr)

	msg, err := fstream.Get(context.Background())
	assert.Nil(t, msg)
	assert.EqualError(t, err, fallbackErrors[2].Error())
}

func TestAppendFallbacksCStream_Timeout(t *testing.T) {
	suspend := 100 * time.Millisecond
	fakeErr := errors.New("stream error")
	fallbackErrors := []error{
		errors.New("Fallback error 0"),
		errors.New("Fallback error 1"),
		errors.New("Fallback error 2"),
	}
	run := func(timeoutMs int) bool {
		mstream := &MockCStream{}
		fallbacks := []CStreamFallback{
			func(ctx context.Context, err error) (Message, error) {
				assert.EqualError(t, err, fakeErr.Error())
				tm := time.NewTimer(suspend)
				defer tm.Stop()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-tm.C:
				}
				return nil, fallbackErrors[0]
			},
			func(ctx context.Context, err error) (Message, error) {
				assert.EqualError(t, err, fallbackErrors[0].Error())
				tm := time.NewTimer(suspend)
				defer tm.Stop()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-tm.C:
				}
				return nil, fallbackErrors[0]
			},
			func(ctx context.Context, err error) (Message, error) {
				assert.EqualError(t, err, fallbackErrors[1].Error())
				tm := time.NewTimer(suspend)
				defer tm.Stop()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-tm.C:
				}
				return nil, fallbackErrors[2]
			},
		}

		fstream := AppendFallbacksCStream(mstream, fallbacks...)

		mstream.On("Get", mock.Anything).Return((*fakeMsg)(nil),
			func(ctx context.Context) error {
				tm := time.NewTimer(suspend)
				defer tm.Stop()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-tm.C:
				}
				return fakeErr
			})

		ctx, cancel := context.WithTimeout(context.Background(),
			time.Duration(timeoutMs)*time.Millisecond)
		defer cancel()
		_, err := fstream.Get(ctx)
		return err == context.DeadlineExceeded
	}
	config := &quick.Config{
		// [1ms, 3*suspend)
		Values: genBoundInt(1, 300),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestAppendFallbacksCStream_UpstreamSucceed(t *testing.T) {
	mstream := &MockCStream{}
	fallback := func(_ context.Context, err error) (Message, error) {
		assert.FailNow(t, "shouldn't run")
		return nil, nil
	}

	fstream := AppendFallbacksCStream(mstream, fallback)

	mm := &fakeMsg{"0"}
	mstream.On("Get", mock.Anything).Return(mm, nil)

	msg, err := fstream.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, mm.ID(), msg.ID())
}

func TestAppendFallbacksCStream_FallThrough(t *testing.T) {
	fakeErr := errors.New("stream error")
	fallbackErr := errors.New("fallback error")
	mm := &fakeMsg{"123"}
	mstream := &MockCStream{}
	fallbacks := []CStreamFallback{
		func(_ context.Context, err error) (Message, error) {
			assert.EqualError(t, err, fakeErr.Error())
			return nil, fallbackErr
		},
		func(_ context.Context, err error) (Message, error) {
			assert.EqualError(t, err, fallbackErr.Error())
			return mm, nil
		},
		func(_ context.Context, err error) (Message, error) {
			assert.FailNow(t, "shouldn't run")
			return nil, nil
		},
	}

	fstream := AppendFallbacksCStream(mstream, fallbacks...)

	mstream.On("Get", mock.Anything).Return((*fakeMsg)(nil), fakeErr)

	msg, err := fstream.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, mm.ID(), msg.ID())
}

func TestAppendFallbacksCStream_FirstHandlerSucceed(t *testing.T) {
	mhandler := &MockCHandler{}
	fallback := func(_ context.Context, msg Message, err error) (Message, error) {
		assert.FailNow(t, "shouldn't run")
		return nil, nil
	}

	fstream := AppendFallbacksCHandler(mhandler, fallback)

	mm := &fakeMsg{"0"}
	mhandler.On("Handle", mock.Anything, mock.Anything).Return(mm, nil)

	msg, err := fstream.Handle(context.Background(), mm)
	assert.NoError(t, err)
	assert.Equal(t, mm.ID(), msg.ID())
}

func TestAppendHandlersCHandler(t *testing.T) {
	// Append multiple Handlers to a Handler. Handlers should run in sequence.
	mhandler := &MockCHandler{}
	mhandlers := []CHandler{&MockCHandler{}, &MockCHandler{}, &MockCHandler{}}
	handler := AppendHandlersCHandler(mhandler, mhandlers...)
	id := 0
	mm := &fakeMsg{id: strconv.Itoa(id)}
	mhandler.On("Handle", mock.Anything, mock.Anything).Return(mm, nil)

	for _, h := range mhandlers {
		h.(*MockCHandler).On("Handle", mock.Anything, mock.Anything).Return(
			func(_ context.Context, msg Message) Message {
				i, _ := strconv.Atoi(msg.ID())
				return &fakeMsg{id: strconv.Itoa(i + 1)}
			}, nil)
	}

	msg, err := handler.Handle(context.Background(), mm)
	assert.NoError(t, err)
	assert.Equal(t, strconv.Itoa(id+len(mhandlers)), msg.ID())
}

func TestAppendHandlersCHandler_Timeout(t *testing.T) {
	suspend := 100 * time.Millisecond
	run := func(timeoutMs int) bool {
		mhandler := &MockCHandler{}
		mhandlers := []CHandler{&MockCHandler{}, &MockCHandler{}, &MockCHandler{}}
		handler := AppendHandlersCHandler(mhandler, mhandlers...)
		id := 0
		mm := &fakeMsg{id: strconv.Itoa(id)}
		var maybeErr error
		mhandler.On("Handle", mock.Anything, mock.Anything).Return(
			func(ctx context.Context, _ Message) Message {
				tm := time.NewTimer(suspend)
				defer tm.Stop()
				select {
				case <-ctx.Done():
					maybeErr = ctx.Err()
					return nil
				case <-tm.C:
				}
				return mm
			}, func(_ context.Context, _ Message) error {
				return maybeErr
			})

		for _, h := range mhandlers {
			var maybeErr error
			h.(*MockCHandler).On("Handle", mock.Anything, mock.Anything).Return(
				func(ctx context.Context, msg Message) Message {
					tm := time.NewTimer(suspend)
					defer tm.Stop()
					select {
					case <-ctx.Done():
						maybeErr = ctx.Err()
						return nil
					case <-tm.C:
					}
					i, _ := strconv.Atoi(msg.ID())
					return &fakeMsg{id: strconv.Itoa(i + 1)}
				}, func(_ context.Context, _ Message) error {
					return maybeErr
				})
		}

		ctx, cancel := context.WithTimeout(context.Background(),
			time.Duration(timeoutMs)*time.Millisecond)
		defer cancel()
		_, err := handler.Handle(ctx, mm)
		return err == context.DeadlineExceeded
	}
	config := &quick.Config{
		// [1ms, 3*suspend)
		Values: genBoundInt(1, 300),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestAppendHandlersCHandler_SimpleHandler(t *testing.T) {
	// AppendHandlerHandler accepts SimpleHandlers and functions converted to
	// SimpleHandler
	mhandler := &MockCHandler{}
	id := "0"
	id2 := "1"
	id3 := "2"

	var h1 SimpleCHandler = func(_ context.Context, msg Message) (Message, error) {
		assert.Equal(t, id, msg.ID())
		return &fakeMsg{id2}, nil
	}

	h2 := func(_ context.Context, msg Message) (Message, error) {
		assert.Equal(t, id2, msg.ID())
		return &fakeMsg{id3}, nil
	}

	handler := AppendHandlersCHandler(mhandler, h1, (SimpleCHandler)(h2))

	mm := &fakeMsg{id}
	mhandler.On("Handle", mock.Anything, mock.Anything).Return(mm, nil)

	msg, err := handler.Handle(context.Background(), mm)
	assert.NoError(t, err)
	assert.Equal(t, id3, msg.ID())
}

func TestAppendHandlersCHandler_FallThrough(t *testing.T) {
	// Append multiple Handlers to a Handler. Handlers should run in sequence.
	// One failing Handler would bypass all subsequent Handlers.
	mhandler := &MockCHandler{}
	mhandlers := []CHandler{&MockCHandler{}, &MockCHandler{}, &MockCHandler{}}
	handler := AppendHandlersCHandler(mhandler, mhandlers...)
	id := 0
	mm := &fakeMsg{id: strconv.Itoa(id)}
	mhandler.On("Handle", mock.Anything, mock.Anything).Return(mm, nil)

	mhandlers[0].(*MockCHandler).On("Handle", mock.Anything, mock.Anything).Return(
		func(_ context.Context, msg Message) Message {
			id++
			i, _ := strconv.Atoi(msg.ID())
			return &fakeMsg{id: strconv.Itoa(i + 1)}
		}, nil)

	fakeErr := errors.New("handler failed")
	mhandlers[1].(*MockCHandler).On("Handle", mock.Anything, mock.Anything).
		Return((*fakeMsg)(nil), fakeErr)

	mhandlers[2].(*MockCHandler).On("Handle", mock.Anything, mock.Anything).Return(
		func(_ context.Context, _ Message) Message {
			assert.FailNow(t, "shouldn't run")
			return nil
		}, nil)

	msg, err := handler.Handle(context.Background(), mm)
	assert.Nil(t, msg)
	assert.EqualError(t, err, fakeErr.Error())
	assert.Equal(t, 1, id)
}

func TestAppendHandlersCHandler_FirstHandlerFail(t *testing.T) {
	// A failing Handler would not trigger subsequent Handlers
	mhandler := &MockCHandler{}
	mhandler2 := &MockCHandler{}
	handler := AppendHandlersCHandler(mhandler, mhandler2)

	fakeErr := errors.New("handler failed")
	mhandler.On("Handle", mock.Anything, mock.Anything).Return((*fakeMsg)(nil), fakeErr)
	mhandler2.On("Handle", mock.Anything, mock.Anything).Return(
		func(_ context.Context, _ Message) Message {
			assert.FailNow(t, "shouldn't run")
			return nil
		}, nil)

	msg, err := handler.Handle(context.Background(), &fakeMsg{"123"})
	assert.Nil(t, msg)
	assert.EqualError(t, err, fakeErr.Error())
}

func TestAppendFallbacksCHandler(t *testing.T) {
	fakeErr := errors.New("handler error")
	fallbackErrors := []error{
		errors.New("Fallback error 0"),
		errors.New("Fallback error 1"),
		errors.New("Fallback error 2"),
	}
	mhandler := &MockCHandler{}
	mm := &fakeMsg{"123"}
	fallbacks := []CHandlerFallback{
		func(_ context.Context, msg Message, err error) (Message, error) {
			assert.Equal(t, mm.ID(), msg.ID())
			assert.EqualError(t, err, fakeErr.Error())
			return nil, fallbackErrors[0]
		},
		func(_ context.Context, msg Message, err error) (Message, error) {
			assert.Equal(t, mm.ID(), msg.ID())
			assert.EqualError(t, err, fallbackErrors[0].Error())
			return nil, fallbackErrors[1]
		},
		func(_ context.Context, msg Message, err error) (Message, error) {
			assert.Equal(t, mm.ID(), msg.ID())
			assert.EqualError(t, err, fallbackErrors[1].Error())
			return nil, fallbackErrors[2]
		},
	}

	fhandler := AppendFallbacksCHandler(mhandler, fallbacks...)

	mhandler.On("Handle", mock.Anything, mock.Anything).Return((*fakeMsg)(nil), fakeErr)

	msg, err := fhandler.Handle(context.Background(), mm)
	assert.Nil(t, msg)
	assert.EqualError(t, err, fallbackErrors[2].Error())
}

func TestAppendFallbacksCHandler_Timeout(t *testing.T) {
	suspend := 100 * time.Millisecond
	fakeErr := errors.New("handler error")
	fallbackErrors := []error{
		errors.New("Fallback error 0"),
		errors.New("Fallback error 1"),
		errors.New("Fallback error 2"),
	}
	run := func(timeoutMs int) bool {
		mhandler := &MockCHandler{}
		mm := &fakeMsg{"123"}
		fallbacks := []CHandlerFallback{
			func(ctx context.Context, msg Message, err error) (Message, error) {
				assert.Equal(t, mm.ID(), msg.ID())
				assert.EqualError(t, err, fakeErr.Error())
				tm := time.NewTimer(suspend)
				defer tm.Stop()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-tm.C:
				}
				return nil, fallbackErrors[0]
			},
			func(ctx context.Context, msg Message, err error) (Message, error) {
				assert.Equal(t, mm.ID(), msg.ID())
				assert.EqualError(t, err, fallbackErrors[0].Error())
				tm := time.NewTimer(suspend)
				defer tm.Stop()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-tm.C:
				}
				return nil, fallbackErrors[1]
			},
			func(ctx context.Context, msg Message, err error) (Message, error) {
				assert.Equal(t, mm.ID(), msg.ID())
				assert.EqualError(t, err, fallbackErrors[1].Error())
				tm := time.NewTimer(suspend)
				defer tm.Stop()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-tm.C:
				}
				return nil, fallbackErrors[2]
			},
		}

		fhandler := AppendFallbacksCHandler(mhandler, fallbacks...)

		mhandler.On("Handle", mock.Anything, mock.Anything).Return((*fakeMsg)(nil),
			func(ctx context.Context, _ Message) error {
				tm := time.NewTimer(suspend)
				defer tm.Stop()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-tm.C:
				}
				return fakeErr
			})

		ctx, cancel := context.WithTimeout(context.Background(),
			time.Duration(timeoutMs)*time.Millisecond)
		defer cancel()
		_, err := fhandler.Handle(ctx, mm)
		return err == context.DeadlineExceeded
	}
	config := &quick.Config{
		// [1ms, 3*suspend)
		Values: genBoundInt(1, 300),
	}
	if err := quick.Check(run, config); err != nil {
		t.Error(err)
	}
}

func TestAppendFallbacksCHandler_FallThrough(t *testing.T) {
	fakeErr := errors.New("stream error")
	fallbackErr := errors.New("fallback error")
	mm := &fakeMsg{"123"}
	mhandler := &MockCHandler{}
	fallbacks := []CHandlerFallback{
		func(_ context.Context, msg Message, err error) (Message, error) {
			assert.Equal(t, mm.ID(), msg.ID())
			assert.EqualError(t, err, fakeErr.Error())
			return nil, fallbackErr
		},
		func(_ context.Context, msg Message, err error) (Message, error) {
			assert.Equal(t, mm.ID(), msg.ID())
			assert.EqualError(t, err, fallbackErr.Error())
			return mm, nil
		},
		func(_ context.Context, msg Message, err error) (Message, error) {
			assert.FailNow(t, "shouldn't run")
			return nil, nil
		},
	}

	fhandler := AppendFallbacksCHandler(mhandler, fallbacks...)

	mhandler.On("Handle", mock.Anything, mock.Anything).Return((*fakeMsg)(nil), fakeErr)

	msg, err := fhandler.Handle(context.Background(), mm)
	assert.NoError(t, err)
	assert.Equal(t, mm.ID(), msg.ID())
}
