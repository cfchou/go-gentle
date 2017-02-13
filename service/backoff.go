// vim:fileencoding=utf-8
package service

import (
	"time"
	"sync"
)

type BackOffImpl struct {
	lock sync.Mutex
	gen BackOffGenerator
	backoffs []time.Duration
	next time.Time
	last_update time.Time
}

type BackOffGenerator func() []time.Duration

func NewBackOffImpl(gen BackOffGenerator) *BackOffImpl {
	return &BackOffImpl{
		gen:gen,
		backoffs: []time.Duration{},
		next: time.Now(),
		last_update: time.Now(),
	}
}

func (s *BackOffImpl) nextBackOff() time.Duration {
	if !s.isBackingOff() {
		panic("Not in backoff state")
	}
	return s.backoffs[0]
}

func (s *BackOffImpl) genBackOffs() {
	s.backoffs = s.gen()
	if len(s.backoffs) == 0 {
		panic("GenerateBackOffs() length is 0")
	}
}

func (s *BackOffImpl) enableBackOff() {
	s.genBackOffs()
}

func (s *BackOffImpl) disableBackOff() {
	s.backoffs = []time.Duration{}
}

func (s *BackOffImpl) isBackingOff() bool {
	return len(s.backoffs) > 0
}

func (s *BackOffImpl) advanceBackOff() {
	if !s.isBackingOff() {
		panic("Not in backoff state")
	}
	if len(s.backoffs) == 1 {
		s.genBackOffs()
	} else {
		s.backoffs = s.backoffs[1:]
	}
}

func (s *BackOffImpl) updateWith(when time.Time, err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !when.Before(s.last_update) {
		// When times are equal, switching(or keeping) back-off off is
		// favoured.
		if err != nil && !s.isBackingOff() && !when.Equal(s.last_update) {
			// Back-off switch on.
			s.genBackOffs()
			s.next = s.next.Add(s.nextBackOff())
			s.advanceBackOff()
		} else if err == nil && s.isBackingOff() {
			// Back-off switch off.
			s.disableBackOff()
			s.next = time.Now()
		}
		s.last_update = when
	}
}

func (s *BackOffImpl) Run(work func() error) error {
	for {
		s.lock.Lock()
		if !s.isBackingOff() {
			s.lock.Unlock()
			break
		}
		// isBackingOff
		now := time.Now()
		if !now.Before(s.next) {
			if s.isBackingOff() {
				s.next = s.next.Add(s.nextBackOff())
				s.advanceBackOff()
			}
			s.lock.Unlock()
			break
		}
		// now before s.next
		wait := s.next.Sub(now)
		s.lock.Unlock()
		time.Sleep(wait)
	}
	err := work()
	end := time.Now()
	s.updateWith(end, err)
	return err
}

func (s *BackOffImpl) RunWithin(timeout time.Duration,
	work func(time.Duration) error) (error, bool) {

	finish_at := time.Now().Add(timeout)

	finish_when := time.NewTimer(timeout)
	cond := sync.Cond{}
	waiting := make(chan *struct{}, 1)
	x := func() {
		for {
			s.lock.Lock()

			if !s.isBackingOff() {
				s.lock.Unlock()
				waiting <- true
				break
			}

			// isBackingOff
			now := time.Now()
			if !now.Before(finish_at) || !s.next.Before(finish_at) {
				s.lock.Unlock()
				waiting <- false
				break
			}
			if !now.Before(s.next) {
				if s.isBackingOff() {
					s.next = s.next.Add(s.nextBackOff())
					s.advanceBackOff()
				}
				s.lock.Unlock()
				waiting <- true
				break
			}
			wait := s.next.Sub(now)
			s.lock.Unlock()
			time.Sleep(wait)
		}

	}

	for {
		s.lock.Lock()
		if !s.isBackingOff() {
			s.lock.Unlock()
			break
		}

		// isBackingOff
		now := time.Now()
		if !now.Before(finish_at) || !s.next.Before(finish_at) {
			s.lock.Unlock()
			return nil, false
		}
		if !now.Before(s.next) {
			if s.isBackingOff() {
				s.next = s.next.Add(s.nextBackOff())
				s.advanceBackOff()
			}
			s.lock.Unlock()
			break
		}
		wait := s.next.Sub(now)
		s.lock.Unlock()
		time.Sleep(wait)
	}
	begin := time.Now()
	if !begin.Before(finish_at) {
		return nil, false
	}

	switch {
	case <-finish_when.C:

	}

	err := work(finish_at.Sub(begin))
	end := time.Now()
	go s.updateWith(end, err)
	return err, true
}


