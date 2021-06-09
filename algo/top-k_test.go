package algo

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"
)

func copyArray(array []int) []int {
	return append([]int{}, array...)
}

func sortedCopy(array []int) []int {
	copied := copyArray(array)
	sort.Ints(copied)
	return copied
}

func TestKthMax(t *testing.T) {
	array := []int{4, 5, 5, 7, 5, 1, 2, 3, 6, 2, 101, 2, 3, 1, 2, 2, 3, 4}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(array), func(i, j int) {
		Ints(array).Swap(i, j)
	})
	assert := testifyassert.New(t)
	for k := 2; k <= len(array)+2; k++ {
		aa := copyArray(array)
		kth := KthMaxInPlace(Ints(aa), k)
		fmt.Printf("%dth max: %d\n", k, kth)

		if k <= len(array) {
			assert.Equal(sortedCopy(array)[len(array)-k], kth)
		}
		//fmt.Printf("k_%d: %v\n", k, sortInts(a[:utils.minInt(k, len(a))]))
	}
}

func TestMinK(t *testing.T) {
	array := []int{4, 5, 5, 7, 5, 1, 2, 3, 6, 2, 101, 2, 3, 1, 2, 2, 3, 4}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(array), func(i, j int) {
		Ints(array).Swap(i, j)
	})
	assert := testifyassert.New(t)
	for k := 2; k <= len(array)+2; k++ {
		aa := copyArray(array)
		minKIntegers := MinK(aa, k)

		if k <= len(array) {
			sort.Ints(minKIntegers)
			assert.Equal(sortedCopy(array)[0:k], minKIntegers)
		}
		//fmt.Printf("k_%d: %v\n", k, sortInts(a[:utils.minInt(k, len(a))]))
	}
}
