package transaction

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/leisurelyrcxf/spermwhale/bench"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

// Deprecated
type keyEventHolder struct {
	// Deprecated
	keyEventWaitersMu sync.Mutex
	// Deprecated
	rollbackedKey2Success map[string]bool
	// Deprecated
	keyEventWaiters map[string][]*KeyEventWaiter
}

type Transaction struct {
	// Deprecated
	*keyEventHolder

	sync.RWMutex

	ID types.TxnId
	types.AtomicTxnState

	db types.KV

	writtenKeys             concurrency.Future
	txnRecordMaxReadVersion uint64
	terminated              chan struct{}

	GC func(*Transaction)
}

func newTransaction(id types.TxnId, db types.KV, destroy func(*Transaction)) *Transaction {
	return &Transaction{
		ID:             id,
		db:             db,
		AtomicTxnState: types.NewAtomicTxnState(types.TxnStateUncommitted),
		terminated:     make(chan struct{}),
		GC:             destroy,
	}
}

var invalidKeyWaiters = make([]*KeyEventWaiter, 0, 0)

func isInvalidKeyWaiters(waiters []*KeyEventWaiter) bool {
	return len(waiters) == 0 && waiters != nil
}

func (t *Transaction) ClearWriteIntent(ctx context.Context, key string, opt types.KVCCUpdateMetaOption) (err error) {
	// NOTE: OK even if opt.IsReadModifyWriteRollbackOrClearReadKey() or kv.db.Set failed TODO needs test against kv.db.Set() failed.
	t.Lock() // NOTE: Lock is must though seems not needed
	t.SetTxnStateUnsafe(types.TxnStateCommitted)
	t.Unlock()

	if opt.IsReadOnlyKey() {
		return nil
	}

	if err = t.db.UpdateMeta(ctx, key, t.ID.Version(), opt.ToKV()); err != nil {
		if glog.V(3) {
			glog.Errorf("txn-%d clear write intent of key '%s' failed: %v", t.ID, key, err)
		}
		return err
	}
	if glog.V(60) {
		glog.Infof("txn-%d clear write intent of key '%s' succeeded, cost: %s", t.ID, key, bench.Elapsed())
	}
	t.doneOnce(key, false, opt.IsOperatedByDifferentTxn(), "ClearWriteIntent")
	return nil
}

func (t *Transaction) RollbackKey(ctx context.Context, key string, opt types.KVCCRollbackKeyOption) (err error) {
	t.Lock()
	if !t.IsAborted() {
		t.SetTxnStateUnsafe(types.TxnStateRollbacking)
	}
	t.Unlock()

	if opt.IsReadOnlyKey() {
		return nil
	}

	if err = t.db.RollbackKey(ctx, key, t.ID.Version()); err != nil {
		if glog.V(3) {
			glog.Errorf("txn-%d rollback key '%s' failed: %v", t.ID, key, err)
		}
		return err
	}
	if glog.V(60) {
		glog.Infof("txn-%d rollback key '%s' succeeded, cost: %s", t.ID, key, bench.Elapsed())
	}
	t.doneOnce(key, true, opt.IsOperatedByDifferentTxn(), "RollbackKey")
	return nil
}

func (t *Transaction) GetMaxTxnRecordReadVersion() uint64 {
	return atomic.LoadUint64(&t.txnRecordMaxReadVersion)
}

func (t *Transaction) updateMaxTxnRecordReadVersion(readerVersion uint64) uint64 {
	assert.Must(readerVersion == types.MaxTxnVersion)
	atomic.StoreUint64(&t.txnRecordMaxReadVersion, readerVersion)
	return readerVersion
}

func (t *Transaction) GetTxnRecord(ctx context.Context, opt types.KVCCReadOption) (types.ValueCC, error) {
	// NOTE: the lock is must though seems no needed
	t.RLock()
	var maxReadVersion uint64
	if opt.IsUpdateTimestampCache() {
		maxReadVersion = t.updateMaxTxnRecordReadVersion(opt.ReaderVersion)
	}
	t.RUnlock()

	val, err := t.db.Get(ctx, "", opt.ToKVReadOption())
	if state := t.GetTxnState(); state.IsCommitted() {
		val.SetCommitted()
	} else if state.IsAborted() {
		val.SetAborted()
	}
	if maxReadVersion != 0 {
		return val.WithMaxReadVersion(maxReadVersion), err
	}
	return val.WithMaxReadVersion(t.GetMaxTxnRecordReadVersion()), err // TODO maybe return max reader version to user?
}

func (t *Transaction) SetTxnRecord(ctx context.Context, val types.Value, opt types.KVCCWriteOption) error {
	t.Lock()
	defer t.Unlock()

	if t.GetTxnState() != types.TxnStateUncommitted {
		if t.IsCommitted() {
			glog.Fatalf("txn-%d write txn record after committed", val.Version)
		}
		assert.Must(t.IsAborted())
		glog.V(70).Infof("[Transaction::SetTxnRecord] txn-%d want to insert txn-record after rollbacked", val.Version)
		return errors.ErrWriteKeyAfterTabletTxnRollbacked
	}

	if val.Version < t.txnRecordMaxReadVersion {
		return errors.ErrWriteReadConflict
	}

	if txnKey := types.TxnId(val.Version).String(); t.AddUnsafe(txnKey) {
		glog.V(70).Infof("[Transaction::SetTxnRecord] txn-%d added key '%s'", val.Version, txnKey)
	}

	if err := t.db.Set(ctx, "", val, opt.ToKVWriteOption()); err != nil {
		if glog.V(4) {
			glog.Errorf("[Transaction::SetTxnRecord] txn-%d set txn-record failed: '%v", val.Version, err)
		}
		return err
	}
	if glog.V(60) {
		glog.Infof("[Transaction::SetTxnRecord] txn-%d set txn-record succeeded, cost %s", val.Version, bench.Elapsed())
	}
	return nil
}

func (t *Transaction) RemoveTxnRecord(ctx context.Context, opt types.KVCCRemoveTxnRecordOption) (err error) {
	var (
		action     string
		isRollback = opt.IsRollback()
	)
	if !isRollback {
		action = "clear txn record on commit"

		t.Lock() // NOTE: Lock is must though seems not needed
		t.SetTxnStateUnsafe(types.TxnStateCommitted)
		t.Unlock()
	} else {
		action = "rollback txn record"

		t.Lock()
		if !t.IsAborted() {
			t.SetTxnStateUnsafe(types.TxnStateRollbacking)
		}
		t.Unlock()
	}

	if err = t.db.RemoveTxnRecord(ctx, t.ID.Version()); err != nil {
		if glog.V(4) {
			glog.Errorf("[RemoveTxnRecord][txn-%d] %s failed: %v", t.ID, action, err)
		}
		return err
	}
	if glog.V(60) {
		glog.Infof("[RemoveTxnRecord][txn-%d] %s succeeded, cost %s", t.ID, action, bench.Elapsed())
	}
	t.doneOnce(t.ID.String(), isRollback, opt.IsOperatedByDifferentTxn(), "RemoveTxnRecord")
	return nil
}

func (t *Transaction) AddUnsafe(key string) (addedNewKey bool) {
	assert.Must(!t.IsDoneUnsafe())
	var keyDone bool
	addedNewKey, keyDone = t.writtenKeys.AddUnsafe(key)
	assert.Must(!keyDone)
	return addedNewKey
}

func (t *Transaction) IsDoneUnsafe() bool {
	return t.writtenKeys.IsDoneUnsafe()
}

func (t *Transaction) IsKeyDone(key string) (b bool) {
	t.RLock()
	defer t.RUnlock()

	return t.writtenKeys.IsKeyDoneUnsafe(key)
}

func (t *Transaction) doneOnce(key string, isRollback bool, isOperatedByDifferentTxn bool, caller string) (doneOnce bool) {
	if isOperatedByDifferentTxn {
		return false
	}

	t.Lock()
	if doneOnce = t.writtenKeys.DoneOnceUnsafe(key); doneOnce {
		if isRollback {
			t.SetTxnStateUnsafe(types.TxnStateRollbacked)
		} else {
			assert.Must(t.IsCommitted())
		}
		t.Unlock()

		if glog.V(60) {
			glog.Infof("[Transaction::%s][doneOnce] txn-%d done, state: %s, (all %d written keys include '%s' have been done)", caller, t.ID, t.GetTxnState(), t.writtenKeys.GetAddedKeyCountUnsafe(), key)
		}

		t.GC(t)
		return doneOnce
	}
	t.Unlock()

	return doneOnce
}

// Hide AtomicTxnState::SetTxnState
func (t *Transaction) SetTxnState(_ types.TxnState) (newState types.TxnState, terminateOnce bool) {
	panic(errors.ErrNotSupported)
}

func (t *Transaction) SetTxnStateUnsafe(state types.TxnState) (newState types.TxnState) {
	var terminateOnce bool
	if newState, terminateOnce = t.AtomicTxnState.SetTxnStateUnsafe(state); terminateOnce {
		close(t.terminated)
	}
	return
}

// Hide AtomicTxnState::SetRollbacking
func (t *Transaction) SetRollbacking() (abortOnce bool) {
	panic(errors.ErrNotSupported)
}

func (t *Transaction) WaitTerminateWithTimeout(ctx context.Context, timeout time.Duration) error {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return t.waitTerminate(cctx)
}

func (t *Transaction) waitTerminate(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.terminated:
		assert.Must(t.IsTerminated())
		return nil
	}
}

// Deprecated
func (t *Transaction) registerKeyEventWaiter(key string) (*KeyEventWaiter, KeyEvent, error) {
	t.RLock()
	defer t.RUnlock()

	switch t.GetTxnState() {
	case types.TxnStateCommitted:
		return nil, NewKeyEvent(key, KeyEventTypeClearWriteIntent), nil
	case types.TxnStateRollbacked:
		assert.Must(t.rollbackedKey2Success[key])
		return nil, NewKeyEvent(key, KeyEventTypeVersionRemoved), nil
	case types.TxnStateRollbacking:
		if rollbackSuccess, ok := t.rollbackedKey2Success[key]; ok {
			if rollbackSuccess {
				return nil, NewKeyEvent(key, KeyEventTypeVersionRemoved), nil
			}
			return nil, NewKeyEvent(key, KeyEventTypeRemoveVersionFailed), nil
		}
		fallthrough
	case types.TxnStateUncommitted:
		t.keyEventWaitersMu.Lock()
		defer t.keyEventWaitersMu.Unlock()

		oldWaiters := t.keyEventWaiters[key]
		assert.Must(!isInvalidKeyWaiters(oldWaiters)) // not invalidKeyWaiters
		if len(oldWaiters)+1 > consts.MaxWriteIntentWaitersCapacityPerTxnPerKey {
			return nil, InvalidKeyEvent, errors.Annotatef(errors.ErrWriteIntentQueueFull, "key: %s", key)
		}
		w := newKeyEventWaiter(key)
		if t.keyEventWaiters == nil {
			t.keyEventWaiters = make(map[string][]*KeyEventWaiter)
		}
		t.keyEventWaiters[key] = append(t.keyEventWaiters[key], w)
		return w, InvalidKeyEvent, nil
	default:
		panic(fmt.Sprintf("impossible state %s", t.GetTxnState()))
	}
}

// Deprecated
func (t *Transaction) signalKeyEventUnsafe(event KeyEvent) {
	state := t.GetTxnState()
	if state == types.TxnStateCommitted || state == types.TxnStateRollbacked {
		return
	}
	assert.Must(!t.writtenKeys.IsDoneUnsafe())
	switch event.Type {
	case KeyEventTypeClearWriteIntent:
		state = t.SetTxnStateUnsafe(types.TxnStateCommitted)
	case KeyEventTypeRemoveVersionFailed:
		assert.Must(state == types.TxnStateRollbacking)
		if t.rollbackedKey2Success == nil {
			t.rollbackedKey2Success = make(map[string]bool)
		}
		t.rollbackedKey2Success[event.Key] = false
	case KeyEventTypeVersionRemoved:
		assert.Must(state == types.TxnStateRollbacking)
		if t.rollbackedKey2Success == nil {
			t.rollbackedKey2Success = make(map[string]bool)
		}
		t.rollbackedKey2Success[event.Key] = true
	default:
		panic(fmt.Sprintf("invalid key event type: %s", event.Type))
	}

	t.keyEventWaitersMu.Lock()
	defer t.keyEventWaitersMu.Unlock()

	switch state {
	case types.TxnStateCommitted:
		for key, ws := range t.keyEventWaiters {
			for _, w := range ws {
				w.signal(NewKeyEvent(key, event.Type))
			}
			if t.keyEventWaiters == nil {
				t.keyEventWaiters = map[string][]*KeyEventWaiter{key: invalidKeyWaiters}
			} else {
				t.keyEventWaiters[key] = invalidKeyWaiters // once fired, should no longer register again // TODO remove this in product
			}
		}
	case types.TxnStateRollbacking:
		for _, w := range t.keyEventWaiters[event.Key] {
			w.signal(event)
		}
		if t.keyEventWaiters == nil {
			t.keyEventWaiters = map[string][]*KeyEventWaiter{event.Key: invalidKeyWaiters}
		} else {
			t.keyEventWaiters[event.Key] = invalidKeyWaiters // once fired, should no longer register again // TODO remove this in product
		}
	default:
		panic(fmt.Sprintf("Transaction::signalKeyEventUnsafe: invalid Transaction state: %s", t.GetTxnState()))
	}
	return
}

// Deprecated
func (t *Transaction) getWaiterCounts() (waiterCount, waiterKeyCount int) {
	t.keyEventWaitersMu.Lock()
	defer t.keyEventWaitersMu.Unlock()

	for _, ws := range t.keyEventWaiters {
		waiterCount += len(ws)
	}
	return waiterCount, len(t.keyEventWaiters)
}
