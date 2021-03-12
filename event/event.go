package event

import (
	goContext "context"
	"fmt"
	"sync"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

type Type int

const (
	Invalid Type = iota
	WriteIntentCleared
	VersionRemoved
)

type WriteIntentWaiter struct {
	waitress chan Type
}

func newWriteIntentWaiter() *WriteIntentWaiter {
	return &WriteIntentWaiter{
		waitress: make(chan Type, 1),
	}
}

func (w *WriteIntentWaiter) Wait(ctx goContext.Context) (Type, error) {
	select {
	case <-ctx.Done():
		return Invalid, ctx.Err()
	case event := <-w.waitress:
		return event, nil
	}
}

func (w *WriteIntentWaiter) Fire(event Type) {
	w.waitress <- event
	close(w.waitress)
}

type WriteIntentWaiters struct {
	sync.Mutex

	firedEvent Type

	waiters []*WriteIntentWaiter
}

func NewWriteIntentWaiters() *WriteIntentWaiters {
	return &WriteIntentWaiters{
		firedEvent: Invalid,
	}
}

func (ws *WriteIntentWaiters) AppendWaiter(w *WriteIntentWaiter) *WriteIntentWaiter {
	ws.Lock()
	defer ws.Unlock()

	if ws.firedEvent != Invalid {
		w.Fire(ws.firedEvent)
		return w
	}

	ws.waiters = append(ws.waiters, w)
	return w
}

func (ws *WriteIntentWaiters) FireEvent(event Type) {
	ws.Lock()
	defer ws.Unlock()

	assert.Must(ws.firedEvent == Invalid || (event == ws.firedEvent && len(ws.waiters) == 0))

	for _, w := range ws.waiters {
		w.Fire(event)
	}
	ws.waiters = nil

	ws.firedEvent = event
}

type WriteIntentWaiterManager struct {
	m concurrency.ConcurrentMap
}

func NewWriteIntentWaiterManager() *WriteIntentWaiterManager {
	tc := &WriteIntentWaiterManager{}
	tc.m.Initialize(64)
	return tc
}

func (wm *WriteIntentWaiterManager) RegisterWaiter(key string, writtenTxn uint64) *WriteIntentWaiter {
	return wm.m.GetLazy(writeIntentEventKey(key, writtenTxn), func() interface{} {
		return NewWriteIntentWaiters()
	}).(*WriteIntentWaiters).AppendWaiter(newWriteIntentWaiter())
}

func (wm *WriteIntentWaiterManager) FireEvent(key string, writtenTxn uint64, event Type) {
	wm.m.GetLazy(writeIntentEventKey(key, writtenTxn), func() interface{} {
		return NewWriteIntentWaiters()
	}).(*WriteIntentWaiters).FireEvent(event)
}

func (wm *WriteIntentWaiterManager) GC(key string, writtenTxn uint64) {
	wm.m.Del(writeIntentEventKey(key, writtenTxn))
}

func (wm *WriteIntentWaiterManager) Close() {
	wm.m.Clear()
}

func writeIntentEventKey(key string, writtenTxn uint64) string {
	return fmt.Sprintf("%s-%d", key, writtenTxn)
}
