// vim:fileencoding=utf-8
package service

import (
	"github.com/cfchou/porter/service"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var Log = service.Log.New()

// Provide an interface for mocking
type ReceiveSpec interface {
	ToReceiveMessageInput() (*sqs.ReceiveMessageInput, error)
}


