package sync2

import (
	"sync/atomic"
)

// AtomicInt32 is a wrapper with a simpler interface around atomic.(Add|Store|Load|CompareAndSwap)Int32 functions.
type AtomicInt32 struct {
	int32
}

// NewAtomicInt32 initializes a new AtomicInt32 with a given value.
func NewAtomicInt32(n int32) AtomicInt32 {
	return AtomicInt32{n}
}

// Add atomically adds n to the value.
func (i *AtomicInt32) Add(n int32) int32 {
	return atomic.AddInt32(&i.int32, n)
}

// Set atomically sets n as new value.
func (i *AtomicInt32) Set(n int32) {
	atomic.StoreInt32(&i.int32, n)
}

// Get atomically returns the current value.
func (i *AtomicInt32) Get() int32 {
	return atomic.LoadInt32(&i.int32)
}

// CompareAndSwap automatically swaps the old with the new value.
func (i *AtomicInt32) CompareAndSwap(oldVal, newVal int32) (swapped bool) {
	return atomic.CompareAndSwapInt32(&i.int32, oldVal, newVal)
}

// AtomicInt64 is a wrapper with a simpler interface around atomic.(Add|Store|Load|CompareAndSwap)Int64 functions.
type AtomicInt64 struct {
	int64
}

// NewAtomicInt64 initializes a new AtomicInt64 with a given value.
func NewAtomicInt64(n int64) AtomicInt64 {
	return AtomicInt64{n}
}

// Add atomically adds n to the value.
func (i *AtomicInt64) Add(n int64) int64 {
	return atomic.AddInt64(&i.int64, n)
}

// Set atomically sets n as new value.
func (i *AtomicInt64) Set(n int64) {
	atomic.StoreInt64(&i.int64, n)
}

// Get atomically returns the current value.
func (i *AtomicInt64) Get() int64 {
	return atomic.LoadInt64(&i.int64)
}

// CompareAndSwap automatically swaps the old with the new value.
func (i *AtomicInt64) CompareAndSwap(oldVal, newVal int64) (swapped bool) {
	return atomic.CompareAndSwapInt64(&i.int64, oldVal, newVal)
}

// AtomicBool gives an atomic boolean variable.
type AtomicBool struct {
	int32
}

//NewAtomicBool initializes a new AtomicBool with a given value.
func NewAtomicBool(n bool) AtomicBool {
	if n {
		return AtomicBool{1}
	}
	return AtomicBool{0}
}

// Set atomically sets n as new value.
func (i *AtomicBool) Set(n bool) {
	if n {
		atomic.StoreInt32(&i.int32, 1)
	} else {
		atomic.StoreInt32(&i.int32, 0)
	}
}

// Get atomically returns the current value.
func (i *AtomicBool) Get() bool {
	return atomic.LoadInt32(&i.int32) != 0
}

// CompareAndSwap automatically swaps the old with the new value.
func (i *AtomicBool) CompareAndSwap(o, n bool) bool {
	var old, new int32
	if o {
		old = 1
	}
	if n {
		new = 1
	}
	return atomic.CompareAndSwapInt32(&i.int32, old, new)
}
