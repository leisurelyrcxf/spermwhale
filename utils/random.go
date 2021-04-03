package utils

import (
	"math/rand"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"
)

func RandomPeriod(unit time.Duration, min, max int) time.Duration {
	rand.Seed(time.Now().UnixNano())
	return unit * time.Duration(min+rand.Intn(max-min+1))
}

func RandomSequence(n int) []int {
	seq := make([]int, n)
	RandomShuffle(seq)
	return seq
}

func RandomShuffle(seq []int) {
	assert.Must(len(seq) > 0)
	for i := range seq {
		seq[i] = i
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(seq), func(i, j int) {
		seq[i], seq[j] = seq[j], seq[i]
	})
}

var characters = []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g'}

func RandomKey(length int) string {
	rand.Seed(time.Now().UnixNano())
	bytes := make([]byte, length)
	for i := range bytes {
		bytes[i] = characters[rand.Intn(len(characters))]
	}
	return string(bytes)
}
