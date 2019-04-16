package dsc

import (
	"sync"
	"sync/atomic"
	"time"
)

type timeWindow struct {
	Start time.Time
	End   time.Time
}

//Limiter represents resource limter
type Limiter struct {
	count    int64
	max      int
	duration time.Duration
	win      *timeWindow
	mux      *sync.Mutex
}

//Acquire checks if limit for current time window was not exhausted or sleep
func (l *Limiter) Acquire() {

	for {
		window := l.window()
		if int(atomic.AddInt64(&l.count, 1)) <= l.max {
			return
		}
		duration := window.End.Sub(time.Now())
		if duration > 0 {
			time.Sleep(duration)
		}
	}
}

func (l *Limiter) window() *timeWindow {
	l.mux.Lock()
	defer l.mux.Unlock()
	if time.Now().After(l.win.End) {
		l.win.Start = time.Now()
		l.win.End = time.Now().Add(l.duration)
		atomic.StoreInt64(&l.count, 0)
	}
	return l.win
}

//NewLimiter creates a new limiter
func NewLimiter(duration time.Duration, max int) *Limiter {
	if max == 0 {
		max = 1
	}
	return &Limiter{
		mux:      &sync.Mutex{},
		duration: duration,
		win: &timeWindow{
			Start: time.Now(),
			End:   time.Now().Add(duration),
		},
		max: max,
	}
}
