package utils

import "testing"

func TestRandomShuffle(t *testing.T) {
	v := RandomSequence(4)
	t.Logf("v: %v", v)
}
