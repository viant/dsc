package dsc

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestLimiter_Acquire(t *testing.T) {

	limiter := NewLimiter(10*time.Millisecond, 10)

	startTime := time.Now()
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer waitGroup.Done()
			limiter.Acquire()
		}()

	}
	waitGroup.Wait()
	elapsed := time.Now().Sub(startTime)
	assert.True(t, elapsed >= time.Second)
}
