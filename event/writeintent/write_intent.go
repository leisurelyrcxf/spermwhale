package writeintent

import (
	goContext "context"
	"fmt"
	"sync"
	"time"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

type Type int

const (
	Invalid Type = iota
	Cleared
	Removed
)

type Waiter struct {
	waitress chan Type
}

func newWaiter() *Waiter {
	return &Waiter{
		waitress: make(chan Type, 1),
	}
}

func (w *Waiter) Wait(ctx goContext.Context, timeout time.Duration) (Type, error) {
	waitCtx, cancel := goContext.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-waitCtx.Done():
		return Invalid, waitCtx.Err()
	case event := <-w.waitress:
		return event, nil
	}
}

func (w *Waiter) signal(event Type) {
	w.waitress <- event
	close(w.waitress)
}

type waiters struct {
	sync.Mutex

	waiters    []*Waiter
	firedEvent Type
}

func newWaiters() *waiters {
	return &waiters{
		firedEvent: Invalid,
	}
}

func (ws *waiters) appendWaiter(w *Waiter) (*Waiter, error) {
	ws.Lock()
	defer ws.Unlock()

	if ws.firedEvent != Invalid {
		w.signal(ws.firedEvent)
		return w, nil
	}

	if len(ws.waiters)+1 > consts.MaxWriteIntentWaitersCapacityPerKeyAndVersion {
		return nil, errors.ErrWriteIntentQueueFull
	}
	ws.waiters = append(ws.waiters, w)
	return w, nil
}

func (ws *waiters) fireEvent(event Type) {
	ws.Lock()
	defer ws.Unlock()

	assert.Must(ws.firedEvent == Invalid || (event == ws.firedEvent && len(ws.waiters) == 0))

	for _, w := range ws.waiters {
		w.signal(event)
	}
	ws.waiters = nil

	ws.firedEvent = event
}

type Manager struct {
	m concurrency.ConcurrentMap
}

func NewManager() *Manager {
	tc := &Manager{}
	tc.m.Initialize(64)
	return tc
}

func (wm *Manager) RegisterWaiter(key string, writtenTxn uint64) (*Waiter, error) {
	return wm.m.GetLazy(eventKey(key, writtenTxn), func() interface{} {
		return newWaiters()
	}).(*waiters).appendWaiter(newWaiter())
}

func (wm *Manager) FireEvent(key string, writtenTxn uint64, event Type) {
	wm.m.GetLazy(eventKey(key, writtenTxn), func() interface{} {
		return newWaiters()
	}).(*waiters).fireEvent(event)
}

func (wm *Manager) GC(key string, writtenTxn uint64) {
	wm.m.Del(eventKey(key, writtenTxn))
}

func (wm *Manager) Close() {
	wm.m.Clear()
}

func eventKey(key string, writtenTxn uint64) string {
	return fmt.Sprintf("%s-%d", key, writtenTxn)
}
