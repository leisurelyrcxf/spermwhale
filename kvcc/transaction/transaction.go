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

	Unref func(*Transaction)
}

func newTransaction(id types.TxnId, db types.KV, unref func(*Transaction)) *Transaction {
	t := &Transaction{
		ID:             id,
		db:             db,
		AtomicTxnState: types.NewAtomicTxnState(types.TxnStateUncommitted),
		terminated:     make(chan struct{}),
		Unref:          unref,
	}
	t.writtenKeys.Initialize()
	return t
}

func (t *Transaction) ClearWriteIntent(ctx context.Context, key string, opt types.KVCCUpdateMetaOption) (err error) {
	// NOTE: OK even if opt.IsReadModifyWriteRollbackOrClearReadKey() or kv.db.Set failed TODO needs test against kv.db.Set() failed.
	t.Lock() // NOTE: Lock is must though seems not needed
	assert.Must(opt.TxnInternalVersion == t.writtenKeys.MustGetDBMetaUnsafe(key).InternalVersion)
	t.SetTxnStateUnsafe(types.TxnStateCommitted)
	t.Unlock()

	var readonly bool
	if readonly, err = clearWriteIntent(ctx, key, t.ID.Version(), opt, t.db); err == nil && !readonly {
		t.doneOnce(key, false, opt.IsOperatedByDifferentTxn(), "ClearWriteIntent")
	}
	return err
}

func clearWriteIntent(ctx context.Context, key string, version uint64, opt types.KVCCUpdateMetaOption, db types.KV) (readOnly bool, _ error) {
	if opt.IsReadOnlyKey() {
		return true, nil
	}
	if err := db.UpdateMeta(ctx, key, version, opt.ToKV()); err != nil {
		if glog.V(3) {
			glog.Errorf("txn-%d clear write intent of key '%s' failed: %v", version, key, err)
		}
		return false, err
	}
	// TODO add state CommittedCleared maybe
	if glog.V(60) {
		glog.Infof("txn-%d clear write intent of key '%s' succeeded, cost: %s", version, key, bench.Elapsed())
	}
	return false, nil
}

func (t *Transaction) RollbackKey(ctx context.Context, key string, opt types.KVCCRollbackKeyOption) (err error) {
	t.Lock()
	if !t.IsAborted() {
		t.SetTxnStateUnsafe(types.TxnStateRollbacking)
	}
	t.Unlock()

	var readonly bool
	if readonly, err = rollbackKey(ctx, key, t.ID.Version(), opt, t.db); err == nil && !readonly {
		t.doneOnce(key, true, opt.IsOperatedByDifferentTxn(), "RollbackKey")
	}
	return err
}

func rollbackKey(ctx context.Context, key string, version uint64, opt types.KVCCRollbackKeyOption, db types.KV) (readonly bool, _ error) {
	if opt.IsReadOnlyKey() {
		return true, nil
	}
	if err := db.RollbackKey(ctx, key, version); err != nil {
		if glog.V(3) {
			glog.Errorf("txn-%d rollback key '%s' failed: %v", version, key, err)
		}
		return false, err
	}
	if glog.V(60) {
		glog.Infof("txn-%d rollback key '%s' succeeded, cost: %s", version, key, bench.Elapsed())
	}
	return false, nil
}

func (t *Transaction) GetMaxTxnRecordReadVersion() uint64 {
	return atomic.LoadUint64(&t.txnRecordMaxReadVersion)
}

func (t *Transaction) updateMaxTxnRecordReadVersion(readerVersion uint64) uint64 {
	assert.Must(readerVersion == types.MaxTxnVersion)
	atomic.StoreUint64(&t.txnRecordMaxReadVersion, readerVersion)
	return readerVersion
}

func (t *Transaction) MustGetMeta(key string) (m types.Meta, isAborted bool) {
	t.RLock()
	defer t.RUnlock()

	m = t.writtenKeys.MustGetDBMetaUnsafe(key).WithVersion(t.ID.Version())
	m.UpdateTxnState(t.GetTxnState())
	return m, m.IsAborted()
}

func (t *Transaction) GetMeta(key string) (types.Meta, error) {
	t.RLock()
	defer t.RUnlock()

	dbMeta, ok := t.writtenKeys.GetDBMetaUnsafe(key)
	if !ok {
		assert.Must(!t.IsCommitted())
		return types.Meta{}, errors.ErrKeyOrVersionNotExist
	}
	m := dbMeta.WithVersion(t.ID.Version())
	m.UpdateTxnState(t.GetTxnState())
	return m, nil
}

func (t *Transaction) HasPositiveInternalVersion(key string, version types.TxnInternalVersion) bool {
	t.RLock()
	defer t.RUnlock()

	return t.writtenKeys.HasPositiveInternalVersion(key, version)
}

func (t *Transaction) GetTxnRecord(ctx context.Context, opt types.KVCCReadOption) (types.ValueCC, error) {
	var maxReadVersion uint64

	// NOTE: the lock is must though seems no needed
	t.RLock()
	if opt.UpdateTimestampCache {
		maxReadVersion = t.updateMaxTxnRecordReadVersion(opt.ReaderVersion)
	} else {
		maxReadVersion = t.GetMaxTxnRecordReadVersion()
	}
	t.RUnlock()

	val, err := t.db.Get(ctx, "", opt.ToKV())
	val.UpdateTxnState(t.GetTxnState())
	return val.WithMaxReadVersion(maxReadVersion), err // TODO maybe return max reader version to user?
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

	if txnKey := t.ID.String(); t.writtenKeys.MustAddUnsafe(txnKey, val.Meta.ToDB()) {
		glog.V(70).Infof("[Transaction::SetTxnRecord] txn-%d added key '%s'", val.Version, txnKey)
	}

	if err := t.db.Set(ctx, "", val, opt.ToKV()); err != nil {
		if glog.V(4) && err != errors.ErrInject {
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

	if err = removeTxnRecord(ctx, t.ID.Version(), action, t.db); err == nil {
		t.doneOnce(t.ID.String(), isRollback, opt.IsOperatedByDifferentTxn(), "RemoveTxnRecord")
	}
	return err
}

func removeTxnRecord(ctx context.Context, version uint64, action string, db types.KV) (err error) {
	if err = db.RemoveTxnRecord(ctx, version); err != nil {
		if glog.V(4) {
			glog.Errorf("[removeTxnRecord][txn-%d] %s failed: %v", version, action, err)
		}
		return err
	}
	if glog.V(60) {
		glog.Infof("[removeTxnRecord][txn-%d] %s succeeded, cost %s", version, action, bench.Elapsed())
	}
	return nil
}

func (t *Transaction) MustAddWrittenKeyUnsafe(key string, meta types.DBMeta) (addedNewKey bool) {
	return t.writtenKeys.MustAddUnsafe(key, meta)
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
			t.SetTxnStateUnsafe(types.TxnStateRollbackedCleared)
		} else {
			assert.Must(t.IsCommitted())
		}
		t.Unlock()

		if glog.V(60) {
			glog.Infof("[Transaction::%s][doneOnce] txn-%d done, state: %s, (all %d written keys include '%s' have been done)", caller, t.ID, t.GetTxnState(), t.writtenKeys.GetAddedKeyCountUnsafe(), key)
		}

		t.Unref(t)
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
		return nil
	}
}

// Deprecated
type keyEventHolder struct {
	// Deprecated
	keyEventWaitersMu sync.Mutex
	// Deprecated
	rollbackedKey2Success map[string]bool
	// Deprecated
	keyEventWaiters map[string][]*KeyEventWaiter
}

// Deprecated
func (t *Transaction) registerKeyEventWaiter(key string) (*KeyEventWaiter, KeyEvent, error) {
	t.RLock()
	defer t.RUnlock()

	switch t.GetTxnState() {
	case types.TxnStateCommitted:
		return nil, NewKeyEvent(key, KeyEventTypeClearWriteIntent), nil
	case types.TxnStateRollbackedCleared:
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
	if state == types.TxnStateCommitted || state == types.TxnStateRollbackedCleared {
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

// Deprecated
var invalidKeyWaiters = make([]*KeyEventWaiter, 0, 0)

// Deprecated
func isInvalidKeyWaiters(waiters []*KeyEventWaiter) bool {
	return len(waiters) == 0 && waiters != nil
}
