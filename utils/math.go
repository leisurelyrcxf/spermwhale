package utils

import (
	"math"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"
)

func MinInt(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func MinUint64(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func MaxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func MaxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func IsPowerOf2(x int) bool {
	return x > 1 && x&(x-1) == 0
}

func SafeIncr(version *uint64) {
	if cur := *version; cur != math.MaxUint64 {
		*version = cur + 1
	}
}

func SafeDecr(version uint64) uint64 {
	assert.Must(version != 0)
	return version - 1
}
