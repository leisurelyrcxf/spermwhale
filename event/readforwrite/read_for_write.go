package readforwrite

import (
	"container/heap"
	goContext "context"
	"sync"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

type Type int

const (
	Invalid Type = iota
	WriteIntentCleared
	VersionRemoved
	VersionWritten
)

type Reader struct {
	waitress chan struct{}

	version uint64
}

func newReader(readerVersion uint64) *Reader {
	return &Reader{
		waitress: make(chan struct{}),
		version:  readerVersion,
	}
}

func (w *Reader) Wait(ctx goContext.Context, timeout time.Duration) error {
	if w == nil {
		return nil
	}
	waitCtx, cancel := goContext.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-waitCtx.Done():
		return waitCtx.Err()
	case <-w.waitress:
		return nil
	}
}

func (w *Reader) signal() {
	close(w.waitress)
}

type priorityQueue struct {
	sync.Mutex

	waiters []*Reader
}

func newPriorityQueue() *priorityQueue {
	return &priorityQueue{}
}

// Heap Interface
func (ws *priorityQueue) Swap(i, j int) {
	ws.waiters[i], ws.waiters[j] = ws.waiters[j], ws.waiters[i]
}
func (ws *priorityQueue) Less(i, j int) bool {
	return ws.waiters[i].version < ws.waiters[j].version
}
func (ws *priorityQueue) Len() int {
	return len(ws.waiters)
}
func (ws *priorityQueue) Push(x interface{}) {
	ws.waiters = append(ws.waiters, x.(*Reader))
}
func (ws *priorityQueue) Pop() interface{} {
	last := ws.waiters[len(ws.waiters)-1]
	ws.waiters = ws.waiters[0 : len(ws.waiters)-1]
	return last
}

func (ws *priorityQueue) minReader() *Reader {
	if len(ws.waiters) == 0 {
		return nil
	}
	return ws.waiters[0]
}

func (ws *priorityQueue) appendReader(w *Reader) (*Reader, error) {
	ws.Lock()
	defer ws.Unlock()

	if minReader := ws.minReader(); minReader != nil && w.version <= minReader.version {
		assert.Must(w.version < minReader.version)
		return nil, errors.ErrReadForWriteTooStale
	}

	if ws.Len()+1 > consts.MaxReadForWriteQueueCapacityPerKey {
		return nil, errors.ErrReadForWriteQueueFull
	}

	heap.Push(ws, w)
	if ws.Len() == 1 {
		w.signal()
	}
	return w, nil
}

func (ws *priorityQueue) signal(writerVersion uint64) {
	ws.Lock()
	defer ws.Unlock()

	minReader := ws.minReader()
	if minReader == nil {
		return
	}
	if writerVersion != minReader.version {
		return
	}
	heap.Pop(ws)
	if newMinReader := ws.minReader(); newMinReader != nil {
		newMinReader.signal()
	}
}

type Manager struct {
	m concurrency.ConcurrentMap
}

func NewManager() *Manager {
	tc := &Manager{}
	tc.m.Initialize(64)
	return tc
}

func (wm *Manager) AppendReader(key string, readerVersion uint64) (*Reader, error) {
	return wm.m.GetLazy(key, func() interface{} {
		return newPriorityQueue()
	}).(*priorityQueue).appendReader(newReader(readerVersion))
}

func (wm *Manager) Signal(key string, writeVersion uint64, _ Type) {
	wm.m.GetLazy(key, func() interface{} {
		return newPriorityQueue()
	}).(*priorityQueue).signal(writeVersion)
}

func (wm *Manager) Close() {
	wm.m.Clear()
}
