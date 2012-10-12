package limitmap

import (
	"testing"
)

func TestLimitMapRandom(t *testing.T) {
	const N = 10000
	wait := make(chan bool)
	m := NewLimitMap()
	for i := 0; i < N; i++ {
		key := "k" + string((i%7)+0x30)
		go func() {
			m.Acquire(key, 5)
			<-wait
			m.Release(key)
			wait <- true
		}()
		go func() {
			wait <- true
		}()
	}
	for i := 0; i < N; i++ {
		<-wait
	}
}

// See how it scales
func BenchmarkSemaphoreBoth01(b *testing.B) {
	b.StopTimer()
	s := NewSemaphore(1)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s.Acquire()
		s.Release()
	}
}
func BenchmarkSemaphoreBoth10(b *testing.B) {
	b.StopTimer()
	s := NewSemaphore(10)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			s.Acquire()
			s.Release()
		}
	}
}
