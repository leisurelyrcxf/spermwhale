package types

import (
	"container/heap"
	"sync"

	"github.com/leisurelyrcxf/spermwhale/types/basic"

	"github.com/leisurelyrcxf/spermwhale/assert"
)

type Writer struct {
	Version    uint64
	OnUnlocked func()

	writing, clean, rollbacked basic.AtomicBool
	rw                         sync.RWMutex
	Next                       *Writer
}

func NewWriter(version uint64) *Writer {
	return &Writer{Version: version}
}

func (w *Writer) Lock() {
	w.rw.Lock()
	w.writing.Set(true)
}

func (w *Writer) Unlock() {
	w.writing.Set(false)
	w.rw.Unlock()

	w.OnUnlocked()
}

func (w *Writer) Wait() {
	if w != nil && w.writing.Get() {
		w.rw.RLock()
		w.rw.RUnlock()
	}
}

func (w *Writer) MarkClean() {
	w.clean.Set(true)
}

func (w *Writer) MarkRollbacked() {
	w.rollbacked.Set(true)
}

func (w *Writer) IsClean() bool {
	return w.clean.Get()
}

func (w *Writer) IsRollbacked() bool {
	return w.rollbacked.Get()
}

func (w *Writer) IsWriting() bool {
	return w.writing.Get()
}

func (w *Writer) CheckLegalReadVersion(valVersion uint64) {
	assert.Must(valVersion < w.Version)
	assert.Must(w.IsRollbacked())
	assert.Must(w.Next == nil || valVersion >= w.Next.Version || w.Next.IsRollbacked())
}

type Writers struct {
	writers []*Writer
	index   map[uint64]int
}

func (ws *Writers) Initialize()       { ws.index = make(map[uint64]int) }
func (ws Writers) Len() int           { return len(ws.writers) }
func (ws Writers) Less(i, j int) bool { return ws.writers[i].Version < ws.writers[j].Version }
func (ws Writers) Swap(i, j int) {
	wi, wj := ws.writers[i], ws.writers[j]
	ws.writers[i] = wj
	ws.index[wj.Version] = i
	ws.writers[j] = wi
	ws.index[wi.Version] = j
}
func (ws *Writers) Push(x interface{}) {
	w := x.(*Writer)
	ws.writers = append(ws.writers, w)
	ws.index[w.Version] = len(ws.writers) - 1
}
func (ws *Writers) Pop() interface{} {
	old := ws.writers
	n := len(ws.writers)
	ele := old[n-1]
	ws.writers = old[:n-1]
	delete(ws.index, ele.Version)
	return ele
}

type WriterHeap Writers

func (h *WriterHeap) Initialize()    { (*Writers)(h).Initialize() }
func (h *WriterHeap) Push(x *Writer) { heap.Push((*Writers)(h), x) }
func (h *WriterHeap) Remove(writer *Writer) {
	h.MustContain(writer)
	heap.Remove((*Writers)(h), h.index[writer.Version])
}
func (h WriterHeap) Min() *Writer {
	if len(h.writers) == 0 {
		return nil
	}
	return h.writers[0]
}
func (h *WriterHeap) MustContain(writers ...*Writer) {
	for _, writer := range writers {
		idx, ok := h.index[writer.Version]
		assert.Must(ok)
		assert.Must(h.writers[idx] == writer)
	}
}
