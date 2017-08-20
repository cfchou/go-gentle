package gentle

import (
	"context"
	"errors"
	"fmt"
)

func ExampleAppendHandlersStream() {
	var upstream SimpleStream = func(_ context.Context) (Message, error) {
		msg := SimpleMessage("abc")
		fmt.Println("stream returns msg:", msg.ID())
		return msg, nil
	}

	var repeatHandler SimpleHandler = func(_ context.Context, msg Message) (Message, error) {
		msgOut := SimpleMessage(msg.ID() + "+")
		fmt.Printf("handle msg: %s, returns msg: %s\n", msg.ID(), msgOut.ID())
		return msgOut, nil
	}

	stream := AppendHandlersStream(upstream, repeatHandler, repeatHandler)
	msg, _ := stream.Get(context.Background())
	fmt.Println("msg finally returned:", msg.ID())
	// Output:
	// stream returns msg: abc
	// handle msg: abc, returns msg: abc+
	// handle msg: abc+, returns msg: abc++
	// msg finally returned: abc++
}

func ExampleAppendFallbacksStream() {
	var upstream SimpleStream = func(_ context.Context) (Message, error) {
		fmt.Println("upstream fails to return a msg")
		return nil, errors.New("stream error")
	}

	var fallbackA StreamFallback = func(_ context.Context, err error) (Message, error) {
		fmt.Println("fallbackA fails to correct the error:", err)
		return nil, errors.New("fallbackA error")
	}

	var fallbackB StreamFallback = func(_ context.Context, err error) (Message, error) {
		fmt.Println("fallbackB successfully deals with the error:", err)
		return SimpleMessage("abc"), nil
	}

	stream := AppendFallbacksStream(upstream, fallbackA, fallbackB)
	msg, _ := stream.Get(context.Background())
	fmt.Println("msg finally returned:", msg.ID())

	// Output:
	// upstream fails to return a msg
	// fallbackA fails to correct the error: stream error
	// fallbackB successfully deals with the error: fallbackA error
	// msg finally returned: abc
}

func ExampleAppendHandlersHandler() {
	var repeatHandler SimpleHandler = func(_ context.Context, msg Message) (Message, error) {
		msgOut := SimpleMessage(msg.ID() + "+")
		fmt.Printf("handle msg: %s, returns msg: %s\n", msg.ID(), msgOut.ID())
		return msgOut, nil
	}
	handler := AppendHandlersHandler(repeatHandler, repeatHandler, repeatHandler)
	msg, _ := handler.Handle(context.Background(), SimpleMessage("abc"))
	fmt.Println("msg finally returned:", msg.ID())
	// Output:
	// handle msg: abc, returns msg: abc+
	// handle msg: abc+, returns msg: abc++
	// handle msg: abc++, returns msg: abc+++
	// msg finally returned: abc+++
}

func ExampleAppendFallbacksHandler() {
	var firstHandler SimpleHandler = func(_ context.Context, msg Message) (Message, error) {
		fmt.Println("handler err caused by msg:", msg.ID())
		return nil, errors.New("handler err")
	}
	var fallbackA HandlerFallback = func(_ context.Context, msg Message, err error) (Message, error) {
		fmt.Println("fallbackA fails to deal with msg:", msg.ID())
		return nil, errors.New("fallback err")
	}
	var fallbackB HandlerFallback = func(_ context.Context, msg Message, err error) (Message, error) {
		msgOut := SimpleMessage("xyz")
		fmt.Printf("fallbackB successfully deals with msg: %s, returns msg: %s\n",
			msg.ID(), msgOut.ID())
		return msgOut, nil
	}

	handler := AppendFallbacksHandler(firstHandler, fallbackA, fallbackB)
	msg, _ := handler.Handle(context.Background(), SimpleMessage("abc"))
	fmt.Println("msg finally returned:", msg.ID())
	// Output:
	// handler err caused by msg: abc
	// fallbackA fails to deal with msg: abc
	// fallbackB successfully deals with msg: abc, returns msg: xyz
	// msg finally returned: xyz
}
