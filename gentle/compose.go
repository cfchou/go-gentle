package gentle

import "context"

// StreamFallback is a fallback function of an error.
type StreamFallback func(error) (Message, error)

type SimpleStream func() (Message, error)

func (r SimpleStream) Get() (Message, error) {
	return r()
}

func AppendHandlersStream(stream Stream, handlers ...Handler) Stream {
	var simple SimpleStream = func() (Message, error) {
		msg, err := stream.Get()
		if err != nil {
			return nil, err
		}
		for _, handler := range handlers {
			msg, err = handler.Handle(msg)
			if err != nil {
				return nil, err
			}
		}
		return msg, nil
	}
	return simple
}

func AppendFallbacksStream(stream Stream, fallbacks ...StreamFallback) Stream {
	var simple SimpleStream = func() (Message, error) {
		msg, err := stream.Get()
		if err == nil {
			return msg, nil
		}
		for _, fallback := range fallbacks {
			msg, err = fallback(err)
			if err == nil {
				return msg, nil
			}
		}
		return nil, err
	}
	return simple
}

// HandlerFallback is a fallback function of an error and the causal Message of
// that error.
type HandlerFallback func(Message, error) (Message, error)

type SimpleHandler func(Message) (Message, error)

func (r SimpleHandler) Handle(msg Message) (Message, error) {
	return r(msg)
}

func AppendHandlersHandler(handler Handler, handlers ...Handler) Handler {
	var simple SimpleHandler = func(msg Message) (Message, error) {
		msg, err := handler.Handle(msg)
		if err != nil {
			return nil, err
		}
		for _, h := range handlers {
			msg, err = h.Handle(msg)
			if err != nil {
				return nil, err
			}
		}
		return msg, nil
	}
	return simple
}

func AppendFallbacksHandler(handler Handler, fallbacks ...HandlerFallback) Handler {
	var simple SimpleHandler = func(msg Message) (Message, error) {
		// msg is the same for every fallback
		msgOut, err := handler.Handle(msg)
		if err == nil {
			return msgOut, nil
		}
		for _, fallback := range fallbacks {
			msgOut, err = fallback(msg, err)
			if err == nil {
				return msgOut, nil
			}
		}
		return nil, err
	}
	return simple
}

type SimpleCStream func(context.Context) (Message, error)

func (r SimpleCStream) Get(ctx context.Context) (Message, error) {
	return r(ctx)
}

func AppendCHandlersCStream(stream CStream, handlers ...CHandler) CStream {
	var simple SimpleCStream = func(ctx context.Context) (Message, error) {
		msg, err := stream.Get(ctx)
		if err != nil {
			return nil, err
		}
		for _, handler := range handlers {
			msg, err = handler.Handle(ctx, msg)
			if err != nil {
				return nil, err
			}
		}
		return msg, nil
	}
	return simple
}
