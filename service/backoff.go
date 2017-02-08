// vim:fileencoding=utf-8
package service

import (
	"time"
	"gopkg.in/eapache/go-resiliency.v1/retrier"
)

type DefaultBackOff struct {
	expBackOff   []time.Duration
	constBackOff []time.Duration
}

func NewDefaultBackOff(expInit int, expSteps int, constWindow int) BackOff {
	return &DefaultBackOff{
		expBackOff: retrier.ExponentialBackoff(expSteps,
			IntToMillis(expInit)),
		constBackOff: retrier.ConstantBackoff(1024,
			IntToMillis(constWindow)),
	}
}

func (b *DefaultBackOff) Run(work func() error) error {
	retryExp := retrier.New(b.expBackOff, nil)
	if err := retryExp.Run(work); err == nil {
		return nil
	}
	for {
		retryConst := retrier.New(b.constBackOff, nil)
		if err := retryConst.Run(work); err == nil {
			break
		}
	}
	return nil
}
