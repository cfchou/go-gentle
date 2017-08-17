package gentle

import "context"

type SimpleStream func(context.Context) (Message, error)

func (r SimpleStream) Get(ctx context.Context) (Message, error) {
	return r(ctx)
}

type StreamFallback func(context.Context, error) (Message, error)

func AppendHandlersStream(stream Stream, handlers ...Handler) Stream {
	var simple SimpleStream = func(ctx context.Context) (Message, error) {
		msg, err := stream.Get(ctx)
		if err != nil {
			return nil, err
		}
		// Try to respect context's timeout as much as we can
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		for _, handler := range handlers {
			// always keep the latest msg and err
			msg, err = handler.Handle(ctx, msg)
			if err != nil {
				return nil, err
			}
			// Try to respect context's timeout as much as we can
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		// return the latest msg
		return msg, nil
	}
	return simple
}

func AppendFallbacksStream(stream Stream, fallbacks ...StreamFallback) Stream {
	var simple SimpleStream = func(ctx context.Context) (Message, error) {
		msg, err := stream.Get(ctx)
		if err == nil {
			return msg, nil
		}
		// Try to respect context's timeout as much as we can
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		for _, fallback := range fallbacks {
			// always keep the latest msg and err
			msg, err = fallback(ctx, err)
			if err == nil {
				return msg, nil
			}
			// Try to respect context's timeout as much as we can
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		// return the latest err
		return nil, err
	}
	return simple
}

type SimpleHandler func(context.Context, Message) (Message, error)

func (r SimpleHandler) Handle(ctx context.Context, msg Message) (Message, error) {
	return r(ctx, msg)
}

// HandlerFallback is a fallback function of an error and the causal Message of
// that error.
type HandlerFallback func(context.Context, Message, error) (Message, error)

func AppendHandlersHandler(handler Handler, handlers ...Handler) Handler {
	var simple SimpleHandler = func(ctx context.Context, msg Message) (Message, error) {
		msg, err := handler.Handle(ctx, msg)
		if err != nil {
			return nil, err
		}
		// Try to respect context's timeout as much as we can
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		for _, h := range handlers {
			msg, err = h.Handle(ctx, msg)
			if err != nil {
				return nil, err
			}
			// Try to respect context's timeout as much as we can
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		return msg, nil
	}
	return simple
}

func AppendFallbacksHandler(handler Handler, fallbacks ...HandlerFallback) Handler {
	var simple SimpleHandler = func(ctx context.Context, msg Message) (Message, error) {
		// msg is the same for every fallback
		msgOut, err := handler.Handle(ctx, msg)
		if err == nil {
			return msgOut, nil
		}
		// Try to respect context's timeout as much as we can
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		for _, fallback := range fallbacks {
			// fallback takes the original msg
			msgOut, err = fallback(ctx, msg, err)
			if err == nil {
				return msgOut, nil
			}
			// Try to respect context's timeout as much as we can
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		return nil, err
	}
	return simple
}
