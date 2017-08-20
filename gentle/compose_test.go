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

func TestAppendHandlersStream(t *testing.T) {
	// Append multiple Handlers to a Stream. Handlers run in sequence.
	// The first handler accepts Message returned by the upstream, and then
	// every subsequent handler accepts a Message returned by the previous
	// handler.
	mstream := &MockStream{}
	mhandlers := []Handler{&MockHandler{}, &MockHandler{}, &MockHandler{}}
	stream := AppendHandlersStream(mstream, mhandlers...)
	id := 0
	mm := SimpleMessage(strconv.Itoa(id))
	mstream.On("Get", mock.Anything).Return(mm, nil)

	for _, mhandler := range mhandlers {
		mhandler.(*MockHandler).On("Handle", mock.Anything, mock.Anything).Return(
			func(_ context.Context, msg Message) Message {
				// Every handler accepts a Message returned by the
				// previous one.
				i, _ := strconv.Atoi(msg.ID())
				return SimpleMessage(strconv.Itoa(i + 1))
			}, nil)
	}

	msg, err := stream.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, strconv.Itoa(id+len(mhandlers)), msg.ID())
}

func TestAppendHandlersStream_Timeout(t *testing.T) {
	// Timeout can happen
	suspend := 100 * time.Millisecond
	run := func(timeoutMs int) bool {
		mstream := &MockStream{}
		mhandlers := []Handler{&MockHandler{}, &MockHandler{}, &MockHandler{}}
		stream := AppendHandlersStream(mstream, mhandlers...)
		id := 0
		mm := SimpleMessage(strconv.Itoa(id))
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
			mhandler.(*MockHandler).On("Handle", mock.Anything, mock.Anything).Return(
				func(ctx context.Context, msg Message) Message {
					tm := time.NewTimer(suspend)
					defer tm.Stop()
					select {
					case <-ctx.Done():
						maybeErr = ctx.Err()
						return nil
					case <-tm.C:
						i, _ := strconv.Atoi(msg.ID())
						return SimpleMessage(strconv.Itoa(i + 1))
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

func TestAppendHandlersStream_FallThrough_Upstream(t *testing.T) {
	// Failed Stream.Get() would not trigger subsequent Handlers.
	mstream := &MockStream{}
	mhandler := &MockHandler{}
	stream := AppendHandlersStream(mstream, mhandler)

	fakeErr := errors.New("stream failed")
	mstream.On("Get", mock.Anything).Return((*SimpleMessage)(nil), fakeErr)
	mhandler.On("Handle", mock.Anything, mock.Anything).Return(
		func(_ context.Context, _ Message) Message {
			assert.FailNow(t, "shouldn't run")
			return nil
		}, nil)

	msg, err := stream.Get(context.Background())
	assert.Nil(t, msg)
	assert.EqualError(t, err, fakeErr.Error())
}

func TestAppendHandlersStream_FallThrough_Handlers(t *testing.T) {
	// One failed Handler.Handle() would bypass all subsequent Handlers.
	mstream := &MockStream{}
	mhandlers := []Handler{&MockHandler{}, &MockHandler{}, &MockHandler{}}
	stream := AppendHandlersStream(mstream, mhandlers...)
	id := 0
	mm := SimpleMessage(strconv.Itoa(id))
	mstream.On("Get", mock.Anything).Return(mm, nil)

	mhandlers[0].(*MockHandler).On("Handle", mock.Anything, mock.Anything).
		Return(func(_ context.Context, msg Message) Message {
			id++
			i, _ := strconv.Atoi(msg.ID())
			return SimpleMessage(strconv.Itoa(i + 1))
		}, nil)

	fakeErr := errors.New("handler failed")
	mhandlers[1].(*MockHandler).On("Handle", mock.Anything, mock.Anything).
		Return((*SimpleMessage)(nil), fakeErr)

	mhandlers[2].(*MockHandler).On("Handle", mock.Anything, mock.Anything).
		Return(func(_ context.Context, _ Message) Message {
			assert.FailNow(t, "shouldn't run")
			return nil
		}, nil)

	msg, err := stream.Get(context.Background())
	assert.Nil(t, msg)
	assert.EqualError(t, err, fakeErr.Error())
	assert.Equal(t, 1, id)
}

func TestAppendFallbacksStream(t *testing.T) {
	// Append multiple StreamFallbacks to a Stream. StreamFallbacks run in
	// sequence.
	// The first fallback accepts Message returned by the upstream, and then
	// every subsequent fallback accepts an error returned by the previous
	// fallback.
	fakeErr := errors.New("stream error")
	fallbackErrors := []error{
		errors.New("Fallback error 0"),
		errors.New("Fallback error 1"),
		errors.New("Fallback error 2"),
	}
	mstream := &MockStream{}
	fallbacks := []StreamFallback{
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

	fstream := AppendFallbacksStream(mstream, fallbacks...)

	mstream.On("Get", mock.Anything).Return((*SimpleMessage)(nil), fakeErr)

	msg, err := fstream.Get(context.Background())
	assert.Nil(t, msg)
	assert.EqualError(t, err, fallbackErrors[2].Error())
}

func TestAppendFallbacksStream_Timeout(t *testing.T) {
	// Timeout can happen
	suspend := 100 * time.Millisecond
	fakeErr := errors.New("stream error")
	fallbackErrors := []error{
		errors.New("Fallback error 0"),
		errors.New("Fallback error 1"),
		errors.New("Fallback error 2"),
	}
	run := func(timeoutMs int) bool {
		mstream := &MockStream{}
		fallbacks := []StreamFallback{
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

		fstream := AppendFallbacksStream(mstream, fallbacks...)

		mstream.On("Get", mock.Anything).Return((*SimpleMessage)(nil),
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

func TestAppendFallbacksStream_FallThrough_Upstream(t *testing.T) {
	// Successful Stream.Get() would not trigger subsequent fallbacks.
	mstream := &MockStream{}
	fallback := func(_ context.Context, err error) (Message, error) {
		assert.FailNow(t, "shouldn't run")
		return nil, nil
	}

	fstream := AppendFallbacksStream(mstream, fallback)

	mm := SimpleMessage("123")
	mstream.On("Get", mock.Anything).Return(mm, nil)

	msg, err := fstream.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, mm.ID(), msg.ID())
}

func TestAppendFallbacksStream_FallThrough_Fallbacks(t *testing.T) {
	// One successful fallback would bypass all subsequent fallbacks.
	fakeErr := errors.New("stream error")
	fallbackErr := errors.New("fallback error")
	mm := SimpleMessage("123")
	mstream := &MockStream{}
	fallbacks := []StreamFallback{
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

	fstream := AppendFallbacksStream(mstream, fallbacks...)

	mstream.On("Get", mock.Anything).Return((*SimpleMessage)(nil), fakeErr)

	msg, err := fstream.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, mm.ID(), msg.ID())
}

func TestAppendHandlersHandler(t *testing.T) {
	// Append multiple Handlers to a Handler. Handlers run in sequence.
	// Every handler accepts a Message returned by the previous handler.
	mhandler := &MockHandler{}
	mhandlers := []Handler{&MockHandler{}, &MockHandler{}, &MockHandler{}}
	handler := AppendHandlersHandler(mhandler, mhandlers...)
	id := 0
	mm := SimpleMessage(strconv.Itoa(id))
	mhandler.On("Handle", mock.Anything, mock.Anything).Return(mm, nil)

	for _, h := range mhandlers {
		h.(*MockHandler).On("Handle", mock.Anything, mock.Anything).Return(
			func(_ context.Context, msg Message) Message {
				// Every handler accepts a Message returned by the previous one.
				i, _ := strconv.Atoi(msg.ID())
				return SimpleMessage(strconv.Itoa(i + 1))
			}, nil)
	}

	msg, err := handler.Handle(context.Background(), mm)
	assert.NoError(t, err)
	assert.Equal(t, strconv.Itoa(id+len(mhandlers)), msg.ID())
}

func TestAppendHandlersHandler_Timeout(t *testing.T) {
	suspend := 100 * time.Millisecond
	run := func(timeoutMs int) bool {
		mhandler := &MockHandler{}
		mhandlers := []Handler{&MockHandler{}, &MockHandler{}, &MockHandler{}}
		handler := AppendHandlersHandler(mhandler, mhandlers...)
		id := 0
		mm := SimpleMessage(strconv.Itoa(id))
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
			h.(*MockHandler).On("Handle", mock.Anything, mock.Anything).Return(
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
					return SimpleMessage(strconv.Itoa(i + 1))
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

func TestAppendHandlersHandler_FallThrough(t *testing.T) {
	// One failed Handler.Handle() would bypass all subsequent handlers.
	mhandler := &MockHandler{}
	mhandlers := []Handler{&MockHandler{}, &MockHandler{}, &MockHandler{}}
	handler := AppendHandlersHandler(mhandler, mhandlers...)
	id := 0
	mm := SimpleMessage(strconv.Itoa(id))
	mhandler.On("Handle", mock.Anything, mock.Anything).Return(mm, nil)

	mhandlers[0].(*MockHandler).On("Handle", mock.Anything, mock.Anything).Return(
		func(_ context.Context, msg Message) Message {
			id++
			i, _ := strconv.Atoi(msg.ID())
			return SimpleMessage(strconv.Itoa(i + 1))
		}, nil)

	fakeErr := errors.New("handler failed")
	mhandlers[1].(*MockHandler).On("Handle", mock.Anything, mock.Anything).
		Return((*SimpleMessage)(nil), fakeErr)

	mhandlers[2].(*MockHandler).On("Handle", mock.Anything, mock.Anything).Return(
		func(_ context.Context, _ Message) Message {
			assert.FailNow(t, "shouldn't run")
			return nil
		}, nil)

	msg, err := handler.Handle(context.Background(), mm)
	assert.Nil(t, msg)
	assert.EqualError(t, err, fakeErr.Error())
	assert.Equal(t, 1, id)
}

func TestAppendFallbacksHandler(t *testing.T) {
	// Append multiple HandlerFallbacks to a Handler. HandlerFallbacks run in
	// sequence.
	// handler.Handle() and every fallback takes the same Message.
	// The first fallback takes the error from handler.Handle().
	// Every subsequent fallback accepts an error returned by the previous one.
	fakeErr := errors.New("handler error")
	fallbackErrors := []error{
		errors.New("Fallback error 0"),
		errors.New("Fallback error 1"),
		errors.New("Fallback error 2"),
	}
	mhandler := &MockHandler{}
	mm := SimpleMessage("123")
	fallbacks := []HandlerFallback{
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

	fhandler := AppendFallbacksHandler(mhandler, fallbacks...)

	mhandler.On("Handle", mock.Anything, mock.Anything).Return((*SimpleMessage)(nil), fakeErr)

	msg, err := fhandler.Handle(context.Background(), mm)
	assert.Nil(t, msg)
	assert.EqualError(t, err, fallbackErrors[2].Error())
}

func TestAppendFallbacksHandler_Timeout(t *testing.T) {
	// Timeout can happen
	suspend := 100 * time.Millisecond
	fakeErr := errors.New("handler error")
	fallbackErrors := []error{
		errors.New("Fallback error 0"),
		errors.New("Fallback error 1"),
		errors.New("Fallback error 2"),
	}
	run := func(timeoutMs int) bool {
		mhandler := &MockHandler{}
		mm := SimpleMessage("123")
		fallbacks := []HandlerFallback{
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

		fhandler := AppendFallbacksHandler(mhandler, fallbacks...)

		mhandler.On("Handle", mock.Anything, mock.Anything).Return((*SimpleMessage)(nil),
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

func TestAppendFallbacksHandler_FallThrough(t *testing.T) {
	// One successful fallback would bypass all subsequent fallbacks.
	fakeErr := errors.New("stream error")
	fallbackErr := errors.New("fallback error")
	mm := SimpleMessage("123")
	mhandler := &MockHandler{}
	fallbacks := []HandlerFallback{
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

	fhandler := AppendFallbacksHandler(mhandler, fallbacks...)

	mhandler.On("Handle", mock.Anything, mock.Anything).Return((*SimpleMessage)(nil), fakeErr)

	msg, err := fhandler.Handle(context.Background(), mm)
	assert.NoError(t, err)
	assert.Equal(t, mm.ID(), msg.ID())
}
