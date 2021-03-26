package utils

import "time"

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

func MaxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
