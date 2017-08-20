package gentle

import "context"

// StreamFallback is defined to be a function that takes the error returned by
// Stream.Get() and then either returns a Message or return an error.
type StreamFallback func(context.Context, error) (Message, error)

// AppendHandlersStream appends multiple Handlers to a Stream. Handlers run in
// sequence. The first handler accepts Message returned by the stream, and then
// every subsequent handler accepts a Message returned by the previous handler.
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

// AppendFallbacksStream appends multiple StreamFallbacks to a Stream.
// StreamFallbacks run in sequence. The first fallback accepts Message returned
// by the Stream and then every subsequent fallback accepts an error returned
// by the previous fallback.
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

// HandlerFallback is defined to be a function that takes the error returned by
// Handler.Handle() and the causal Message of that error. It then either returns
// a Message or return an error.
type HandlerFallback func(context.Context, Message, error) (Message, error)

// AppendHandlersHandler appends multiple Handlers to a Handler. Handlers run in
// sequence and every handler accepts a Message returned by the previous handler.
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

// AppendFallbacksHandler appends multiple HandlerFallbacks to a Handler.
// HandlerFallbacks run in sequence.
// The first fallback takes the error from handler.Handle() and every subsequent
// fallback accepts an error returned by the previous one.
// Moreover every fallback also takes the same Message passed to handler.
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
