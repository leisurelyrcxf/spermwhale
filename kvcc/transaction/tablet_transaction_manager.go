package transaction

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

const (
	doneKeyMask    = 0xffffffff00000000
	writtenKeyMask = 0x00000000ffffffff
)

type Event types.TxnState

var InvalidTxnEvent = Event(types.TxnStateInvalid)

func (e Event) GetTxnState() types.TxnState {
	return types.TxnState(e)
}

func (e Event) String() string {
	return e.GetTxnState().String()
}

type Waiter struct {
	waitress chan Event
}

func newWaiter() *Waiter {
	return &Waiter{
		waitress: make(chan Event, 1),
	}
}

func (w *Waiter) Wait(ctx context.Context, timeout time.Duration) (Event, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-waitCtx.Done():
		return InvalidTxnEvent, waitCtx.Err()
	case event := <-w.waitress:
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

	keyInfo uint64

	waitersMu sync.Mutex
	waiters   []*Waiter
}

func newTransaction(id types.TxnId) *transaction {
	return &transaction{
		id:    id,
		state: types.TxnStateUncommitted,
	}
}

func (t *transaction) setStateUnsafe(state types.TxnState) {
	t.state = state
}

func (t *transaction) getStateUnsafe() types.TxnState {
	return t.state
}

func (t *transaction) addWrittenKey() {
	atomic.AddUint64(&t.keyInfo, 1)
}

func (t *transaction) doneWrittenKey() bool {
	for {
		old := atomic.LoadUint64(&t.keyInfo)
		writtenKey, doneKey := old&writtenKeyMask, (old&doneKeyMask)>>32
		newV := ((doneKey + 1) << 32) | writtenKey
		if atomic.CompareAndSwapUint64(&t.keyInfo, old, newV) {
			if doneKey+1 >= writtenKey {
				return true
			}
			return false
		}
	}
}

func (t *transaction) appendWaiter(w *Waiter) (*Waiter, error) {
	t.waitersMu.Lock()
	defer t.waitersMu.Unlock()

	if len(t.waiters)+1 > consts.MaxWriteIntentWaitersCapacityPerTxn {
		return nil, errors.ErrWriteIntentQueueFull
	}
	t.waiters = append(t.waiters, w)
	return w, nil
}

func (t *transaction) signal(event Event) {
	t.waitersMu.Lock()
	defer t.waitersMu.Unlock()

	for _, w := range t.waiters {
		w.signal(event)
	}
	t.waiters = nil
}

func (t *transaction) getWaiters() []*Waiter {
	t.waitersMu.Lock()
	defer t.waitersMu.Unlock()

	return t.waiters
}

type Manager struct {
	*concurrency.TxnLockManager

	writeTxns concurrency.ConcurrentTxnMap

	toClearTxns    chan types.TxnId
	toClearClosed  bool
	toClearMu      sync.RWMutex
	minClearTxnAge time.Duration
	gcDone         chan struct{}
}

const EstimatedQPS = 100000

func NewManager(minClearTxnAge time.Duration) *Manager {
	tm := &Manager{
		TxnLockManager: concurrency.NewTxnLockManager(),
		toClearTxns:    make(chan types.TxnId, EstimatedQPS*(minClearTxnAge/time.Second*2)),
		minClearTxnAge: minClearTxnAge,
		gcDone:         make(chan struct{}),
	}
	tm.writeTxns.Initialize(64)
	go tm.gc()
	return tm
}

func (tm *Manager) AddWrittenKey(id types.TxnId) {
	tm.writeTxns.GetLazy(id, func() interface{} {
		return newTransaction(id)
	}).(*transaction).addWrittenKey()
}

func (tm *Manager) RegisterWaiter(waitForTxnId types.TxnId) (*Waiter, Event, error) {
	tm.RLock(waitForTxnId)
	defer tm.RUnlock(waitForTxnId)

	waitFor := tm.getTxn(waitForTxnId)
	if waitFor == nil {
		return nil, InvalidTxnEvent, errors.ErrTabletWriteTransactionNotFound
	}
	switch waitFor.getStateUnsafe() {
	case types.TxnStateCommitted:
		return nil, Event(types.TxnStateCommitted), nil
	case types.TxnStateRollbacking:
		return nil, Event(types.TxnStateRollbacking), nil
	case types.TxnStateUncommitted:
		w, err := waitFor.appendWaiter(newWaiter())
		return w, InvalidTxnEvent, err
	default:
		panic(fmt.Sprintf("impossible state %s", waitFor.getStateUnsafe()))
	}
}

func (tm *Manager) Signal(txnId types.TxnId, event Event) {
	tm.Lock(txnId)
	defer tm.Unlock(txnId)

	txn := tm.getTxn(txnId)
	if txn == nil {
		return
	}

	switch event.GetTxnState() {
	case types.TxnStateCommitted:
		txn.setStateUnsafe(types.TxnStateCommitted)
	case types.TxnStateRollbacking:
		txn.setStateUnsafe(types.TxnStateRollbacking)
	default:
		panic(fmt.Sprintf("invalid event %s", event))
	}

	txn.signal(event)
}

// DoneKeyUnsafe mark the key done by adding done key count by 1.
// The function is unsafe because commit and rollback may duplicate,
// but the consequence is acceptable
func (tm *Manager) DoneKeyUnsafe(txnId types.TxnId) {
	txn := tm.getTxn(txnId)
	if txn == nil {
		return
	}
	if txn.doneWrittenKey() {
		// TODO remove this in product
		assert.Must(tm.getTxnState(txn).IsTerminated())
		assert.Must(len(txn.getWaiters()) == 0)

		tm.toClearMu.RLock()
		defer tm.toClearMu.RUnlock()

		if !tm.toClearClosed {
			tm.toClearTxns <- txnId
		}
	}
}

func (tm *Manager) Close() {
	tm.toClearMu.Lock()
	defer tm.toClearMu.Unlock()

	if !tm.toClearClosed {
		close(tm.toClearTxns)
		<-tm.gcDone
		tm.toClearClosed = true
	}
	tm.writeTxns.Clear()
}

func (tm *Manager) GetTxnState(txnId types.TxnId) types.TxnState {
	txn := tm.getTxn(txnId)
	if txn == nil {
		return types.TxnStateInvalid
	}
	return tm.getTxnState(txn)
}

func (tm *Manager) gc() {
	defer close(tm.gcDone)

	for toClearTxn := range tm.toClearTxns {
		if age := toClearTxn.Age(); age < tm.minClearTxnAge {
			time.Sleep(tm.minClearTxnAge - age)
		}
		assert.Must(toClearTxn.Age() >= tm.minClearTxnAge)
		tm.writeTxns.Del(toClearTxn)
	}
}

func (tm *Manager) getTxn(txnId types.TxnId) *transaction {
	i, ok := tm.writeTxns.Get(txnId)
	if !ok {
		return nil
	}
	return i.(*transaction)
}

func (tm *Manager) getTxnState(txn *transaction) types.TxnState {
	tm.RLock(txn.id)
	defer tm.RUnlock(txn.id)

	return txn.getStateUnsafe()
}
