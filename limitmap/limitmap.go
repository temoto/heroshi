// Package limitmap provides map of semaphores to limit concurrency against some string keys.
//
// Usage:
// limits := NewLimitMap()
// func process(url *url.URL, rch chan *http.Response) {
// 	// At most 2 concurrent requests to each host.
// 	limits.Acquire(url.Host, 2)
// 	defer limits.Release(url.Host)
//	r, err := http.Get(url.String())
//	rch <- r
// }
// for url := range urlChan {
// 	go process(url, rch)
// }

package limitmap

import (
	"sync"
)

// Internal structure, may be changed.
// Requirements for this data structure:
// * Acquire() will not block until internal counter reaches set maximum number
// * Release() will decrement internal counter and wake up one goroutine blocked on Acquire().
//   Calling Release() when internal counter is zero is programming error, panic.
type Semaphore struct {
	// Number of Acquires - Releases. When this goes to zero, this structure is removed from map.
	// Only updated inside LimitMap.lk lock.
	refs int

	max   uint
	value uint
	wait  sync.Cond
}

func NewSemaphore(max uint) *Semaphore {
	return &Semaphore{
		max:  max,
		wait: sync.Cond{L: new(sync.Mutex)},
	}
}

func (s *Semaphore) Acquire() uint {
	s.wait.L.Lock()
	defer s.wait.L.Unlock()
	for i := 0; ; i++ {
		if uint(s.value)+1 <= s.max {
			s.value++
			return s.value
		}
		s.wait.Wait()
	}
	panic("Unexpected branch")
}

func (s *Semaphore) Release() (result uint) {
	s.wait.L.Lock()
	defer s.wait.L.Unlock()
	s.value--
	if s.value < 0 {
		panic("Semaphore Release without Acquire")
	}
	s.wait.Signal()
	return
}

type LimitMap struct {
	lk     sync.Mutex
	limits map[string]*Semaphore
	wg     sync.WaitGroup
}

func NewLimitMap() *LimitMap {
	return &LimitMap{
		limits: make(map[string]*Semaphore),
	}
}

func (m *LimitMap) Acquire(key string, max uint) {
	m.lk.Lock()
	l, ok := m.limits[key]
	if !ok {
		l = NewSemaphore(max)
		m.limits[key] = l
	}
	l.refs++
	m.lk.Unlock()

	m.wg.Add(1)
	if x := l.Acquire(); x < 0 || x > l.max {
		panic("oia")
	}
}

func (m *LimitMap) Release(key string) {
	m.lk.Lock()
	l, ok := m.limits[key]
	if !ok {
		panic("LimitMap: key not in map. Possible reason: Release without Acquire.")
	}
	l.refs--
	if l.refs < 0 {
		panic("LimitMap internal error: refs < 0.")
	}
	if l.refs == 0 {
		delete(m.limits, key)
	}
	m.lk.Unlock()

	if x := l.Release(); x < 0 || x > l.max {
		panic("oir")
	}
	m.wg.Done()
}

// Wait until all released.
func (m *LimitMap) Wait() {
	m.wg.Wait()
}

func (m *LimitMap) Size() (keys int, total int) {
	m.lk.Lock()
	keys = len(m.limits)
	for _, l := range m.limits {
		total += int(l.value)
	}
	m.lk.Unlock()
	return
}
