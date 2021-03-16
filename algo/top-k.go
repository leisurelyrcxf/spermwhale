package algo

import (
	"math"
	"sort"

	"github.com/leisurelyrcxf/spermwhale/assert"
)

func swap(a []int, i, j int) {
	a[i], a[j] = a[j], a[i]
}

func threeWayPartition(a []int, i, j, pivotIdx int) (left, right int) {
	pivot := a[pivotIdx]
	low, cur, high := i, i, j
	for cur <= high {
		switch {
		case a[cur] > pivot:
			swap(a, cur, high)
			high--
		case a[cur] < pivot:
			swap(a, cur, low)
			low++
			cur++
		default:
			cur++
		}
		//checkThreeWayEternal(a, i, j, low, cur, high, pivot)
	}
	return low - 1, high + 1
}

func threeWayPartitionReverse(a []int, i, j, pivotIdx int) (left, right int) {
	pivot := a[pivotIdx]
	low, cur, high := i, i, j
	for cur <= high {
		switch {
		case a[cur] < pivot:
			swap(a, cur, high)
			high--
		case a[cur] > pivot:
			swap(a, cur, low)
			low++
			cur++
		default:
			cur++
		}
		//checkThreeWayEternal(a, i, j, low, cur, high, pivot)
	}
	return low - 1, high + 1
}

func threeWayPartitionReverseGeneral(a Slice, i, j int) (left, right int) {
	low, cur, high := i, i, j-1
	for cur <= high {
		switch {
		case a.Less(cur, j):
			a.Swap(cur, high)
			high--
		case a.Greater(cur, j):
			a.Swap(cur, low)
			low++
			cur++
		default:
			cur++
		}
	}
	a.Swap(cur, j)
	return low - 1, high + 2
}

type Slice interface {
	sort.Interface
	Greater(i, j int) bool
	Equal(i, j int) bool
	Slice(i, j int) Slice
	At(i int) interface{}
}

type Ints []int

func (a Ints) Len() int              { return len(a) }
func (a Ints) Less(i, j int) bool    { return a[i] < a[j] }
func (a Ints) Swap(i, j int)         { a[i], a[j] = a[j], a[i] }
func (a Ints) Greater(i, j int) bool { return a[i] > a[j] }
func (a Ints) Equal(i, j int) bool   { return a[i] == a[j] }
func (a Ints) Slice(i, j int) Slice  { return a[i:j] }
func (a Ints) At(i int) interface{}  { return a[i] }

// k starts with 1
func KthMaxInPlace(a Slice, k int) interface{} {
	k -= 1
	if k >= a.Len() || k < 0 {
		return nil
	}
	return kthMax(a, k)
}

func kthMax(a Slice, k int) interface{} {
	assert.Must(k < a.Len() && k >= 0)
	if a.Len() == 1 {
		return a.At(0)
	}
	if k == 0 {
		var maxIndex = 0
		for i := 1; i < a.Len(); i++ {
			if a.Greater(i, maxIndex) {
				maxIndex = i
			}
		}
		return a.At(maxIndex)
	}
	left, right := threeWayPartitionReverseGeneral(a, 0, a.Len()-1)
	left += 1
	right -= 1
	if k < left {
		return kthMax(a.Slice(0, left), k)
	}
	if k > right {
		return kthMax(a.Slice(right+1, a.Len()), k-right-1)
	}
	assert.Must(a.Equal(left, right))
	return a.At(left)
}

func KthMin(a []int, k int) int {
	k -= 1
	if k >= len(a) || k < 0 {
		return -1
	}
	return kthMin(a, k)
}

func kthMin(a []int, k int) int {
	assert.Must(k < len(a) && k >= 0)
	if len(a) == 1 {
		return a[0]
	}
	if k == 0 {
		var ret = math.MaxInt64
		for _, ele := range a {
			ret = minInt(ele, ret)
		}
		return ret
	}
	left, right := threeWayPartition(a, 0, len(a)-1, 0)
	left += 1
	right -= 1
	if k < left {
		return kthMin(a[:left], k)
	}
	if k > right {
		return kthMin(a[right+1:], k-right-1)
	}
	assert.Must(a[left] == a[right])
	return a[left]
}

func MinK(a []int, k int) {
	if k >= len(a) {
		return
	}
	left, right := threeWayPartition(a, 0, len(a)-1, 0)
	left += 1
	right -= 1
	if k < left {
		MinK(a[:left], k)
		return
	}
	if k > right+1 {
		MinK(a[right+1:], k-right-1)
		return
	}
}
