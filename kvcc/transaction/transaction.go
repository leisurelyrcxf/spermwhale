package transaction

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/bench"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/basic"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
	"github.com/leisurelyrcxf/spermwhale/utils/trace"
)

type Transaction struct {
	// Deprecated
	*keyEventHolder

	sync.RWMutex

	ID types.TxnId
	types.AtomicTxnState

	db types.KV

	future                  concurrency.Future
	txnRecordRW             sync.RWMutex
	txnRecordMaxReadVersion uint64

	terminated chan struct{}

	unref    func(*Transaction)
	Unreffed basic.AtomicBool
	maxRetry int

	reason string
}

func newTransaction(id types.TxnId, db types.KV, unref func(*Transaction)) *Transaction {
	t := &Transaction{
		ID:             id,
		db:             db,
		AtomicTxnState: types.NewAtomicTxnState(types.TxnStateUncommitted),
		terminated:     make(chan struct{}),
		unref:          unref,
	}
	t.future.Initialize()
	return t
}

func (t *Transaction) ClearWriteIntent(ctx context.Context, key string, opt types.KVCCUpdateMetaOption) (err error) {
	// NOTE: OK even if opt.IsReadModifyWriteRollbackOrClearReadKey() or kv.db.Set failed TODO needs test against kv.db.Set() failed.
	t.Lock() // NOTE: Lock is must though seems not needed
	t.setCommittedUnsafe(false, "clear write intent of key '%s' by upper layer", key)
	futureKey := types.NewTxnKeyUnionKey(key)
	meta := t.future.MustGetDBMeta(futureKey)
	assert.Must(meta.InternalVersion == opt.TxnInternalVersion && meta.IsCommitted())
	t.Unlock()

	if meta.IsCleared() {
		return nil
	}

	var readonly bool
	if readonly, err = clearWriteIntent(ctx, key, t.ID.Version(), opt, t.db); err == nil && !readonly {
		t.doneKey(futureKey, types.KeyStateCommittedCleared)
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
	if glog.V(60) {
		glog.Infof("txn-%d clear write intent of key '%s' succeeded, cost: %s", version, key, bench.Elapsed())
	}
	return false, nil
}

func (t *Transaction) RollbackKey(ctx context.Context, key string, opt types.KVCCRollbackKeyOption) (err error) {
	t.Lock()
	t.SetAbortedUnsafe(false, "rollback key '%s' by upper layer", key)
	futureKey := types.NewTxnKeyUnionKey(key)
	meta, ok := t.future.GetDBMeta(futureKey)
	assert.Must(!ok || meta.IsAborted())
	t.Unlock()

	if meta.IsCleared() {
		return nil
	}

	var readonly bool
	if readonly, err = rollbackKey(ctx, key, t.ID.Version(), opt, t.db); err == nil && !readonly {
		t.doneKey(futureKey, types.KeyStateRollbackedCleared)
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

// Can only use this for IsCommitted() or IsAborted() call, because the info may be not consistent with db layer
// In case of
// 1. Tablet restarted
// 2. Rollback key or Remove txn record didn't get the transaction.
// 3. The cached state is guaranteed to be stale than db state
func (t *Transaction) GetDBMetaUnsafe(key string) types.DBMeta {
	t.RLock()
	dbMeta := t.future.GetDBMetaUnsafe(types.NewTxnKeyUnionKey(key))
	t.RUnlock()
	return dbMeta
}

func (t *Transaction) HasPositiveInternalVersionUnsafe(key string, version types.TxnInternalVersion) bool {
	return t.future.HasPositiveInternalVersion(types.NewTxnKeyUnionKey(key), version)
}

func (t *Transaction) GetTxnRecord(ctx context.Context, opt types.KVCCReadOption) (types.ValueCC, error) {
	var atomicMaxReadVersion uint64

	t.txnRecordRW.RLock() // guarantee mutual exclusion with Transaction::SetTxnRecord()
	if atomicMaxReadVersion = atomic.LoadUint64(&t.txnRecordMaxReadVersion); opt.UpdateTimestampCache && atomicMaxReadVersion < opt.ReaderVersion {
		assert.Must(opt.ReaderVersion == types.MaxTxnVersion)
		atomic.StoreUint64(&t.txnRecordMaxReadVersion, opt.ReaderVersion)
		atomicMaxReadVersion = opt.ReaderVersion
	}
	t.txnRecordRW.RUnlock()

	// TODO if txn is aborted or committed, then needn't read txn record from db
	val, err := t.db.Get(ctx, "", opt.ToKV())
	val.UpdateTxnState(t.GetTxnState())
	return val.WithMaxReadVersion(atomicMaxReadVersion), err
}

func (t *Transaction) SetTxnRecord(ctx context.Context, val types.Value, opt types.KVCCWriteOption) (err error) {
	t.RLock()
	defer t.RUnlock()

	if t.GetTxnState() != types.TxnStateUncommitted {
		if t.IsCommitted() {
			glog.Fatalf("txn-%d write txn record after committed", val.Version)
		}
		assert.Must(t.IsAborted())
		glog.V(70).Infof("[Transaction::SetTxnRecord] txn-%d want to insert txn-record after rollbacked", val.Version)
		return errors.ErrWriteKeyAfterTabletTxnRollbacked
	}

	t.txnRecordRW.Lock()
	defer t.txnRecordRW.Unlock()

	if val.Version < t.txnRecordMaxReadVersion { // TODO has a bug here
		return errors.ErrWriteReadConflict
	}

	if err = t.SetRLocked(ctx, types.NewTxnKeyUnionTxnRecord(t.ID), val, opt); err == nil && glog.V(60) {
		glog.Infof("[Transaction::SetTxnRecord] txn-%d set txn-record succeeded, cost %s", val.Version, bench.Elapsed())
	}
	return err
}

func (t *Transaction) SetRLocked(ctx context.Context, futureKey types.TxnKeyUnion, val types.Value, opt types.KVCCWriteOption) error {
	var setErr error
	if setErr = t.db.Set(ctx, futureKey.Key, val, opt.ToKV()); setErr != nil {
		exists, chkErr := t.checkVersion(ctx, futureKey, t.maxRetry)
		if chkErr != nil {
			t.future.MustAdd(futureKey, val.Meta.ToDB().WithInvalidKeyState())
			if chkErr != errors.ErrDummy {
				glog.Errorf("[Transaction::setRLockedUnsafe] failed to check key '%s' on set error '%v' of version %d : '%v'", futureKey, setErr, val.Version, chkErr)
			}
			return errors.Annotatef(errors.ErrTabletTxnSetFailedKeyStatusUndetermined, "key: '%s', version: %d, set_error: '%v', get_err: '%v'", futureKey, val.Version, setErr, chkErr)
		}
		if !exists {
			glog.V(10).Infof("[Transaction::setRLockedUnsafe] check version %d exists for key '%s' on set error: '%v', and then found key not exists error", val.Version, futureKey, setErr)
			return errors.Annotatef(errors.ErrTabletTxnSetFailedKeyNotFound, "set_err: %v", setErr)
		}
		// assert.Must(checkErr == nil && exists) error pruning
	}
	if t.future.MustAdd(futureKey, val.Meta.ToDB()) && bool(glog.V(60)) {
		if setErr == nil {
			glog.Infof("[KVCC::%s][setRLockedUnsafe] added new key '%s' to txn-%d", trace.CallerFunc(), futureKey, t.ID)
		} else {
			glog.Infof("[KVCC::%s][setRLockedUnsafe] added new key '%s' to txn-%d, got set error '%v' but checked ok", trace.CallerFunc(), futureKey, t.ID, setErr)
		}
	}
	return nil
}

func (t *Transaction) checkVersion(ctx context.Context, futureKey types.TxnKeyUnion, maxRetry int) (exists bool, err error) {
	for i := 0; i < maxRetry && ctx.Err() == nil; i++ {
		var newVal types.Value
		if newVal, err = t.db.Get(ctx, futureKey.Key, types.NewKVReadCheckVersionOption(t.ID.Version()).CondTxnRecord(futureKey.IsTxnRecord())); err == nil || errors.IsNotExistsErr(err) {
			assert.Must(newVal.Version == t.ID.Version())
			return err == nil, nil
		}
		if i < 10 {
			time.Sleep(time.Millisecond)
		} else {
			time.Sleep(time.Second)
		}
	}
	if err == nil {
		err = errors.ErrDummy
	}
	return false, err
}

func (t *Transaction) RemoveTxnRecord(ctx context.Context, opt types.KVCCRemoveTxnRecordOption) (err error) {
	// TODO skip if already removed
	var (
		action            string
		keyStateAfterDone types.KeyState
		futureKey         = types.NewTxnKeyUnionTxnRecord(t.ID)
		dbMeta            types.DBMeta
		ok                bool
	)
	if !opt.IsRollback() {
		keyStateAfterDone, action = types.KeyStateCommittedCleared, "clear txn record on commit"

		t.Lock() // NOTE: Lock is must though seems not needed
		t.setCommittedUnsafe(false, "clear txn record on commit by upper layer")
		dbMeta = t.future.MustGetDBMeta(futureKey)
		t.Unlock()
	} else {
		// TODO maybe skip if txn record not written?
		keyStateAfterDone, action = types.KeyStateRollbackedCleared, "rollback txn record"

		t.Lock()
		t.SetAbortedUnsafe(false, "rollback txn record by upper layer")
		dbMeta, ok = t.future.GetDBMeta(futureKey)
		assert.Must(!ok || dbMeta.IsAborted())
		t.Unlock()
	}

	if dbMeta.IsCleared() {
		return nil
	}

	if err = removeTxnRecord(ctx, t.ID.Version(), action, t.db); err == nil {
		t.doneKey(futureKey, keyStateAfterDone)
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

func (t *Transaction) doneKey(futureKey types.TxnKeyUnion, keyStateAfterDone types.KeyState) {
	t.Lock()
	if t.future.DoneKey(futureKey, keyStateAfterDone) {
		t.future.AssertAllKeysOfState(keyStateAfterDone)
		t.setTxnStateUnsafe(types.TxnState(keyStateAfterDone), true, "assert failed")
		t.Unlock()

		if glog.V(60) {
			glog.Infof("[Transaction::%s][doneKey] txn-%d done, state: %s, (all %d written keys include '%s' have been done)", trace.CallerFunc(), t.ID, t.GetTxnState(), t.future.GetAddedKeyCountUnsafe(), futureKey)
		}

		t.unref(t)
		return
	}
	t.Unlock()
}

func (t *Transaction) SetAbortedUnsafe(assertValidAndTerminated bool, reason string, args ...interface{}) {
	if !t.IsAborted() {
		t.setTxnStateUnsafe(types.TxnStateRollbacking, assertValidAndTerminated, reason, args...)
	}
}

func (t *Transaction) setCommittedUnsafe(assertValidAndTerminated bool, reason string, args ...interface{}) {
	if !t.IsCommitted() {
		t.reason = reason
		t.setTxnStateUnsafe(types.TxnStateCommitted, assertValidAndTerminated, reason, args...)
	}
}

func (t *Transaction) setTxnStateUnsafe(state types.TxnState, assertValidAndTerminated bool, reason string, args ...interface{}) (newState types.TxnState) {
	var terminateOnce bool
	if newState, terminateOnce = t.AtomicTxnState.SetTxnStateUnsafe(state); terminateOnce {
		if glog.V(TabletTransactionVerboseLevel) {
			glog.Infof(fmt.Sprintf("%s due to %s", newState, reason), args...)
		}
		assert.Must(!assertValidAndTerminated)
		close(t.terminated)

		t.future.NotifyTerminated(state, func(futureKey types.TxnKeyUnion, meta *types.DBMeta) {
			assert.Must(meta.IsKeyStateInvalid() && !assertValidAndTerminated)
			if state.IsAborted() {
				meta.ClearInvalidKeyState() // Allowed to clear positive invalid flag if is rollback, 1. prev exists-> version removed 2. prev not exists -> rollback is no op
				return
			}
			if exists, chkErr := t.checkVersion(context.Background(), futureKey, 100); !exists || chkErr != nil {
				if !exists {
					glog.Fatalf("commit a non-exists key '%s'", futureKey)
				}
				if chkErr != nil {
					glog.Fatalf("commit an invalid key '%s' and check failed: '%v'", futureKey, chkErr)
				}
			}
			meta.ClearInvalidKeyState()
		})
	}
	return
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

// Hide AtomicTxnState::SetTxnState
func (t *Transaction) SetTxnState(_ types.TxnState) (newState types.TxnState, terminateOnce bool) {
	panic(errors.ErrNotSupported)
}

// Hide AtomicTxnState::SetTxnStateUnsafe
func (t *Transaction) SetTxnStateUnsafe(_ types.TxnState) (newState types.TxnState, terminateOnce bool) {
	panic(errors.ErrNotSupported)
}

// Hide AtomicTxnState::SetRollbacking
func (t *Transaction) SetRollbacking() (abortOnce bool) {
	panic(errors.ErrNotSupported)
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
	assert.Must(!t.future.IsDone())
	switch event.Type {
	case KeyEventTypeClearWriteIntent:
		state = t.setTxnStateUnsafe(types.TxnStateCommitted, false, "")
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

// Deprecated
func (t *Transaction) GetMetaWeak(key string) (meta types.Meta, err error, valid bool) {
	futureKey := types.NewTxnKeyUnionKey(key)
	dbMeta, ok := t.future.GetDBMeta(futureKey)
	if !ok {
		assert.Must(!t.IsCommitted())
		return types.Meta{}, errors.ErrKeyOrVersionNotExist, true
	}
	// Don't transform error here if IsAborted() because will do that later
	return dbMeta.WithVersion(t.ID.Version()), nil, dbMeta.IsValid()
}

//func (t *Transaction) GetMetaStrong(key string) (meta types.Meta, err error, valid bool) {
//	dbMeta, ok := t.getDBMetaStrong(key)
//	if !ok {
//		assert.Must(!t.IsCommitted())
//		return types.Meta{}, errors.ErrKeyOrVersionNotExist, true
//	}
//	// Don't transform error here if IsAborted() because will do that later
//	return dbMeta.WithVersion(t.ID.Version()), nil, dbMeta.IsValid()
//}
//
//func (t *Transaction) getDBMetaStrong(key string) (dbMeta types.DBMeta, ok bool) {
//	futureKey := types.NewTxnKeyUnionKey(key)
//
//	t.RLock()
//	t.lm.RLock(futureKey)
//	dbMeta, ok = t.future.GetDBMeta(futureKey)
//	t.lm.RUnlock(futureKey)
//	t.RUnlock()
//
//	return dbMeta, ok
//}
