// vim:fileencoding=utf-8
package service

import (
	"github.com/cfchou/porter/service"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/rs/xid"
)

var Log = service.Log.New()

type SqsMessage struct {
	*sqs.Message
	id *string
}

func NewSqsMessage(msg *sqs.Message) *SqsMessage {
	m := &SqsMessage{
		Message:  msg,
	}
	if msg.MessageId != nil {
		m.id = msg.MessageId
	} else {
		m.id = &xid.New().String()
	}
	return m
}

func (m *SqsMessage) Id() string {
	return *m.id
}

type Reader interface {
	ReceiveMessages() ([]service.Message, error)
}

// Provide an interface for mocking
type ReceiveSpec interface {
	ToReceiveMessageInput() (*sqs.ReceiveMessageInput, error)
}


