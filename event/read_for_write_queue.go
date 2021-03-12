package event

import (
	"container/heap"
	goContext "context"
	"sync"

	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

type ReadForWriteEvent struct {
	Type
	WriterVersion uint64
}

func New(typ Type, writerVersion uint64) ReadForWriteEvent {
	return ReadForWriteEvent{
		Type:          typ,
		WriterVersion: writerVersion,
	}
}

var InvalidEvent = New(Invalid, types.MaxTxnVersion)

type ReadForWriteWaiter struct {
	waitress chan ReadForWriteEvent

	reader  types.KVCCReadOption
	waitFor types.TxnId
}

func newWaiter(reader types.KVCCReadOption, waitFor types.TxnId) *ReadForWriteWaiter {
	return &ReadForWriteWaiter{
		waitress: make(chan ReadForWriteEvent, 1),
		reader:   reader,
		waitFor:  waitFor,
	}
}

func (w *ReadForWriteWaiter) Wait(ctx goContext.Context) (ReadForWriteEvent, error) {
	select {
	case <-ctx.Done():
		return InvalidEvent, ctx.Err()
	case event := <-w.waitress:
		return event, nil
	}
}

func (w *ReadForWriteWaiter) GetWaitForVersion() uint64 {
	if w == nil {
		return 0
	}
	return w.waitFor.Version()
}

func (w *ReadForWriteWaiter) Fire(event ReadForWriteEvent) {
	w.waitress <- event
	close(w.waitress)
	//ctx, cancel := goContext.WithTimeout(goContext.Background(), time.Second)
	//defer cancel()
	//
	//select {
	//case w.waitress <- event:
	//	close(w.waitress)
	//	return
	//case <-ctx.Done():
	//	panic(ctx.Err())
	//}
}

type ReadForWriteQueue struct {
	sync.Mutex

	firedEvent map[uint64]ReadForWriteEvent

	waiters []*ReadForWriteWaiter
}

func NewWaiters() *ReadForWriteQueue {
	return &ReadForWriteQueue{
		firedEvent: make(map[uint64]ReadForWriteEvent),
	}
}

func (ws *ReadForWriteQueue) Swap(i, j int) {
	ws.waiters[i], ws.waiters[j] = ws.waiters[j], ws.waiters[i]
}

func (ws *ReadForWriteQueue) Less(i, j int) bool {
	return ws.waiters[i].reader.ReaderVersion < ws.waiters[j].reader.ReaderVersion
}

func (ws *ReadForWriteQueue) Len() int {
	return len(ws.waiters)
}

func (ws *ReadForWriteQueue) Push(x interface{}) {
	ws.waiters = append(ws.waiters, x.(*ReadForWriteWaiter))
}

func (ws *ReadForWriteQueue) Pop() interface{} {
	last := ws.waiters[len(ws.waiters)-1]
	ws.waiters = ws.waiters[0 : len(ws.waiters)-1]
	return last
}

func (ws *ReadForWriteQueue) PopMinReader() *ReadForWriteWaiter {
	return heap.Pop(ws).(*ReadForWriteWaiter)
}

func (ws *ReadForWriteQueue) MinReader() *ReadForWriteWaiter {
	if len(ws.waiters) == 0 {
		return nil
	}
	return ws.waiters[0]
}

func (ws *ReadForWriteQueue) AppendWaiter(w *ReadForWriteWaiter) *ReadForWriteWaiter {
	ws.Lock()
	defer ws.Unlock()

	if ws.Len() == 0 && ws.firedEvent[w.waitFor.Version()].Type != Invalid {
		w.Fire(ws.firedEvent[w.waitFor.Version()])
		return w
	}

	return w
}

func (ws *ReadForWriteQueue) FireEvent(event ReadForWriteEvent) {
	ws.Lock()
	defer ws.Unlock()

	if event.WriterVersion == ws.MinReader().GetWaitForVersion() {
		ws.PopMinReader().Fire(event)
		return
	}

	ws.firedEvent[event.WriterVersion] = event
}

type ReadForWriteQueueManager struct {
	m concurrency.ConcurrentMap
}

func NewWaiterManager() *ReadForWriteQueueManager {
	tc := &ReadForWriteQueueManager{}
	tc.m.Initialize(64)
	return tc
}

func (wm *ReadForWriteQueueManager) AppendWaiter(key string, reader types.KVCCReadOption, waitFor types.TxnId) *ReadForWriteWaiter {
	return wm.m.GetLazy(key, func() interface{} {
		return NewWaiters()
	}).(*ReadForWriteQueue).AppendWaiter(newWaiter(reader, waitFor))
}

func (wm *ReadForWriteQueueManager) FireEvent(key string, event ReadForWriteEvent) {
	wm.m.GetLazy(key, func() interface{} {
		return NewWaiters()
	}).(*ReadForWriteQueue).FireEvent(event)
}

func (wm *ReadForWriteQueueManager) Close() {
	wm.m.Clear()
}
