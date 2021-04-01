package types

import (
	"testing"
)

func TestWriterHeap(t *testing.T) {
	assert := NewAssertion(t)

	h := WriterHeap{}
	h.Initialize()

	w1, w2, w3 := NewWriter(0), NewWriter(100), NewWriter(101)
	h.Push(w3)
	h.Push(w2)
	h.Push(w1)
	h.MustContain(w1, w2, w3)
	assert.Equal(w1, h.Min())

	h.Remove(w2)
	h.MustContain(w1, w3)
	assert.Equal(w1, h.Min())

	h.Remove(w1)
	h.MustContain(w3)
	assert.Equal(w3, h.Min())

	h.Remove(w3)
	assert.Nil(h.Min())
	assert.Empty(h.index)
}

func TestWriterHeap2(t *testing.T) {
	assert := NewAssertion(t)

	h := WriterHeap{}
	h.Initialize()

	w1, w2, w3 := NewWriter(1), NewWriter(2), NewWriter(3)
	h.Push(w1)
	h.Push(w2)
	h.Push(w3)
	h.MustContain(w1, w2, w3)
	assert.Equal(w1, h.Min())

	h.Remove(w2)
	h.MustContain(w1, w3)
	assert.Equal(w1, h.Min())

	h.Remove(w1)
	h.MustContain(w3)
	assert.Equal(w3, h.Min())

	h.Remove(w3)
	assert.Nil(h.Min())
	assert.Empty(h.index)
}
