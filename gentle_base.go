package go_gentle

import "context"

// Message is passed around Streams/Handlers.
type Message interface {
	// ID() should return a unique string representing this Message.
	ID() string
}

// Stream emits Message.
type Stream interface {
	// Get() returns either a Message or an error exclusively.
	Get(context.Context) (Message, error)
}

// Handler transforms a Message.
type Handler interface {
	// Handle() takes a Message as input and then returns either a Message or
	// an error exclusively.
	Handle(context.Context, Message) (Message, error)
}
