package gentle

import "context"

type SimpleCStream func(context.Context) (Message, error)

func (r SimpleCStream) Get(ctx context.Context) (Message, error) {
	return r(ctx)
}

type CStreamFallback func(context.Context, error) (Message, error)

func AppendHandlersCStream(stream CStream, handlers ...CHandler) CStream {
	var simple SimpleCStream = func(ctx context.Context) (Message, error) {
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

func AppendFallbacksCStream(stream CStream, fallbacks ...CStreamFallback) CStream {
	var simple SimpleCStream = func(ctx context.Context) (Message, error) {
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

type SimpleCHandler func(context.Context, Message) (Message, error)

func (r SimpleCHandler) Handle(ctx context.Context, msg Message) (Message, error) {
	return r(ctx, msg)
}

// HandlerFallback is a fallback function of an error and the causal Message of
// that error.
type CHandlerFallback func(context.Context, Message, error) (Message, error)

func AppendHandlersCHandler(handler CHandler, handlers ...CHandler) CHandler {
	var simple SimpleCHandler = func(ctx context.Context, msg Message) (Message, error) {
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

func AppendFallbacksCHandler(handler CHandler, fallbacks ...CHandlerFallback) CHandler {
	var simple SimpleCHandler = func(ctx context.Context, msg Message) (Message, error) {
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
