package bench

import (
	"sync/atomic"
	"time"
)

var StartTime time.Time
var LastTime atomic.Value

func Elapsed() time.Duration {
	last := LastTime.Load()
	if last == nil {
		last = time.Time{}
	}

	now := time.Now()
	elapsed := now.Sub(last.(time.Time))
	LastTime.Store(now)
	return elapsed
}
