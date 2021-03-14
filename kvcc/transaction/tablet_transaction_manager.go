package transaction

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

type Event struct {
	Key   string
	State types.TxnState
}

var InvalidEvent = Event{State: types.TxnStateInvalid}

func NewEvent(key string, state types.TxnState) Event {
	return Event{
		Key:   key,
		State: state,
	}
}

func (e Event) String() string {
	return fmt.Sprintf("key: %s, state: %s", e.Key, e.State.String())
}

type Waiter struct {
	key      string
	waitress chan Event
}

func newWaiter(key string) *Waiter {
	return &Waiter{
		key:      key,
		waitress: make(chan Event, 1),
	}
}

func (w *Waiter) Wait(ctx context.Context, timeout time.Duration) (Event, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-waitCtx.Done():
		return InvalidEvent, waitCtx.Err()
	case event := <-w.waitress:
		assert.Must(event.Key == w.key)
		return event, nil
	}
}

func (w *Waiter) signal(event Event) *Waiter {
	w.waitress <- event
	close(w.waitress)
	return w
}

type transaction struct {
	id    types.TxnId
	state types.TxnState

	waitersMu sync.Mutex
	waiters   map[string][]*Waiter
}

func newTransaction(id types.TxnId) *transaction {
	return &transaction{
		id:      id,
		state:   types.TxnStateUncommitted,
		waiters: make(map[string][]*Waiter),
	}
}

var invalidKeyWaiters = make([]*Waiter, 0, 0)

func isInvalidKeyWaiters(waiters []*Waiter) bool {
	return len(waiters) == 0 && waiters != nil
}

func (t *transaction) registerWaiterOnKey(key string) (*Waiter, error) {
	t.waitersMu.Lock()
	defer t.waitersMu.Unlock()

	oldWaiters := t.waiters[key]
	assert.Must(!isInvalidKeyWaiters(oldWaiters)) // not invalidKeyWaiters
	if len(oldWaiters)+1 > consts.MaxWriteIntentWaitersCapacityPerTxnPerKey {
		return nil, errors.ErrWriteIntentQueueFull
	}
	w := newWaiter(key)
	t.waiters[key] = append(t.waiters[key], w)
	return w, nil
}

func (t *transaction) signal(event Event) {
	t.waitersMu.Lock()
	defer t.waitersMu.Unlock()

	for _, w := range t.waiters[event.Key] {
		w.signal(event)
	}
	t.waiters[event.Key] = invalidKeyWaiters
	// TODO change to
	// delete(t.waiters, event.Key)
	// in product
}

func (t *transaction) getWaiterCounts() (waiterCount, waiterKeyCount int) {
	t.waitersMu.Lock()
	defer t.waitersMu.Unlock()

	for _, ws := range t.waiters {
		waiterCount += len(ws)
	}
	return waiterCount, len(t.waiters)
}

type Manager struct {
	*concurrency.TxnLockManager

	writeTxns concurrency.ConcurrentTxnMap
}

const EstimatedMaxQPS = 100000

func NewManager(minClearTxnAge time.Duration) *Manager {
	tm := &Manager{
		TxnLockManager: concurrency.NewTxnLockManager(),
	}
	tm.writeTxns.Initialize(64, true, EstimatedMaxQPS, minClearTxnAge)
	return tm
}

func (tm *Manager) InsertWriteTransaction(id types.TxnId) {
	if inserted, _ := tm.writeTxns.InsertIfNotExists(id, func() interface{} {
		return newTransaction(id)
	}); inserted {
		tm.writeTxns.GCLater(id)
	}
}

func (tm *Manager) RegisterWaiter(waitForWriteTxnId types.TxnId, key string) (*Waiter, Event, error) {
	tm.RLock(waitForWriteTxnId)
	defer tm.RUnlock(waitForWriteTxnId)

	waitFor := tm.getTxn(waitForWriteTxnId)
	if waitFor == nil {
		return nil, InvalidEvent, errors.Annotatef(errors.ErrTabletWriteTransactionNotFound, "key: %s", key)
	}
	switch waitFor.state {
	case types.TxnStateCommitted, types.TxnStateRollbacking, types.TxnStateRollbacked:
		return nil, NewEvent(key, waitFor.state), nil
	case types.TxnStateUncommitted:
		w, err := waitFor.registerWaiterOnKey(key)
		return w, InvalidEvent, errors.Annotatef(err, "key: %s", key)
	default:
		panic(fmt.Sprintf("impossible state %s", waitFor.state))
	}
}

func (tm *Manager) Signal(writeTxnId types.TxnId, event Event) {
	tm.Lock(writeTxnId)
	txn := tm.getTxn(writeTxnId)
	if txn == nil {
		tm.Unlock(writeTxnId)
		return
	}
	assert.Must(event.State.IsTerminated())
	txn.state = event.State
	tm.Unlock(writeTxnId)

	txn.signal(event)
}

func (tm *Manager) Close() {
	tm.writeTxns.Close()
}

func (tm *Manager) GetTxnState(txnId types.TxnId) types.TxnState {
	txn := tm.getTxn(txnId)
	if txn == nil {
		return types.TxnStateInvalid
	}
	return tm.getTxnState(txn)
}

func (tm *Manager) getTxn(txnId types.TxnId) *transaction {
	i, ok := tm.writeTxns.Get(txnId)
	if !ok {
		return nil
	}
	return i.(*transaction)
}

func (tm *Manager) removeTxn(txnId types.TxnId) {
	tm.writeTxns.Del(txnId)
}

func (tm *Manager) getTxnState(txn *transaction) types.TxnState {
	tm.RLock(txn.id)
	defer tm.RUnlock(txn.id)

	return txn.state
}
