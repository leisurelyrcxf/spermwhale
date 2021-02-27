package concurrency

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

// AtomicUint64 is a wrapper with a simpler interface around atomic.(Add|Store|Load|CompareAndSwap)Uint64 functions.
type AtomicUint64 struct {
	uint64
}

// NewAtomicUint64 initializes a new AtomicUint64 with a given value.
func NewAtomicUint64(n uint64) AtomicUint64 {
	return AtomicUint64{n}
}

// Add atomically adds n to the value.
func (i *AtomicUint64) Add(n uint64) uint64 {
	return atomic.AddUint64(&i.uint64, n)
}

// Set atomically sets n as new value.
func (i *AtomicUint64) Set(n uint64) {
	atomic.StoreUint64(&i.uint64, n)
}

// Get atomically returns the current value.
func (i *AtomicUint64) Get() uint64 {
	return atomic.LoadUint64(&i.uint64)
}

// CompareAndSwap automatically swaps the old with the new value.
func (i *AtomicUint64) CompareAndSwap(oldVal, newVal uint64) (swapped bool) {
	return atomic.CompareAndSwapUint64(&i.uint64, oldVal, newVal)
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
