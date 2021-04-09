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
	txnRecordMaxReadVersion uint64
	terminated              chan struct{}

	lm concurrency.AdvancedLockManagerPartition

	unref    func(*Transaction)
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
	t.lm.Initialize()
	return t
}

func (t *Transaction) ClearWriteIntent(ctx context.Context, key string, opt types.KVCCUpdateMetaOption) (err error) {
	// TODO skip if already cleared
	// NOTE: OK even if opt.IsReadModifyWriteRollbackOrClearReadKey() or kv.db.Set failed TODO needs test against kv.db.Set() failed.
	t.Lock() // NOTE: Lock is must though seems not needed
	t.setCommittedUnsafe(false, "clear write intent of key '%s' by upper layer", key)
	meta := t.future.MustGetDBMeta(key)
	assert.Must(meta.InternalVersion == opt.TxnInternalVersion && meta.IsCommitted())
	t.Unlock()

	var readonly bool
	if readonly, err = clearWriteIntent(ctx, key, t.ID.Version(), opt, t.db); err == nil && !readonly {
		t.doneKey(key, types.KeyStateCommittedCleared, opt.IsOperatedByDifferentTxn())
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
	// TODO skip if already cleared
	t.Lock()
	t.SetAbortedUnsafe(false, "rollback key '%s' by upper layer", key)
	meta, ok := t.future.GetDBMeta(key)
	assert.Must(!ok || meta.IsAborted())
	t.Unlock()

	// TODO maybe skip if key not written?
	var readonly bool
	if readonly, err = rollbackKey(ctx, key, t.ID.Version(), opt, t.db); err == nil && !readonly {
		t.doneKey(key, types.KeyStateRollbackedCleared, opt.IsOperatedByDifferentTxn())
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

// Deprecated
func (t *Transaction) IsKeyDoneUnsafe(futureKey string) (b bool) {
	return t.future.IsKeyDone(futureKey)
}

func (t *Transaction) MustGetMetaWeak(futureKey string) types.Meta {
	return t.future.MustGetDBMeta(futureKey).WithVersion(t.ID.Version())
}

func (t *Transaction) MustGetMetaStrong(futureKey string) types.Meta {
	t.RLock()
	t.lm.RLock(futureKey)
	defer func() {
		t.lm.RUnlock(futureKey)
		t.RUnlock()
	}()

	return t.future.MustGetDBMeta(futureKey).WithVersion(t.ID.Version())
}

func (t *Transaction) GetMetaWeak(futureKey string) (meta types.Meta, err error, valid bool) {
	dbMeta, ok := t.future.GetDBMeta(futureKey)
	if !ok {
		assert.Must(!t.IsCommitted())
		return types.Meta{}, errors.ErrKeyOrVersionNotExist, true
	}
	// Don't transform error here if IsAborted() because will do that later
	return dbMeta.WithVersion(t.ID.Version()), nil, dbMeta.IsValid()
}

func (t *Transaction) GetMetaStrong(futureKey string) (meta types.Meta, err error, valid bool) {
	t.RLock()
	t.lm.RLock(futureKey)
	defer func() {
		t.lm.RUnlock(futureKey)
		t.RUnlock()
	}()

	dbMeta, ok := t.future.GetDBMeta(futureKey)
	if !ok {
		assert.Must(!t.IsCommitted())
		return types.Meta{}, errors.ErrKeyOrVersionNotExist, true
	}
	// Don't transform error here if IsAborted() because will do that later
	return dbMeta.WithVersion(t.ID.Version()), nil, dbMeta.IsValid()
}

func (t *Transaction) GetDBMetaStrongUnsafe(futureKey string) types.DBMeta {
	t.RLock()
	t.lm.RLock(futureKey)
	defer func() {
		t.lm.RUnlock(futureKey)
		t.RUnlock()
	}()

	dbMeta, _ := t.future.GetDBMeta(futureKey)
	return dbMeta
}

func (t *Transaction) HasPositiveInternalVersion(key string, version types.TxnInternalVersion) bool {
	t.RLock()
	t.lm.RLock(key)
	defer func() {
		t.lm.RUnlock(key)
		t.RUnlock()
	}()

	return t.future.HasPositiveInternalVersion(key, version)
}

func (t *Transaction) GetTxnRecord(ctx context.Context, opt types.KVCCReadOption) (types.ValueCC, error) {
	var maxReadVersion uint64

	// NOTE: the lock is must though seems no needed
	t.RLock()
	if opt.UpdateTimestampCache {
		assert.Must(opt.ReaderVersion == types.MaxTxnVersion)
		atomic.StoreUint64(&t.txnRecordMaxReadVersion, opt.ReaderVersion)
		maxReadVersion = opt.ReaderVersion
	} else {
		maxReadVersion = atomic.LoadUint64(&t.txnRecordMaxReadVersion)
	}
	t.RUnlock()

	// TODO if txn is aborted or committed, then needn't read txn record from db
	val, err := t.db.Get(ctx, "", opt.ToKV())
	val.UpdateTxnState(t.GetTxnState())
	return val.WithMaxReadVersion(maxReadVersion), err
}

func (t *Transaction) SetTxnRecord(ctx context.Context, val types.Value, opt types.KVCCWriteOption) (err error) {
	var txnKey = t.ID.String()
	t.Lock() // TODO use RLock instead
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

	if err = t.SetRLocked(ctx, "", txnKey, val, opt); err == nil && glog.V(60) {
		glog.Infof("[Transaction::SetTxnRecord] txn-%d set txn-record succeeded, cost %s", val.Version, bench.Elapsed())
	}
	return err
}

func (t *Transaction) SetRLocked(ctx context.Context, key string, futureKey string, val types.Value, opt types.KVCCWriteOption) error {
	t.lm.Lock(futureKey)
	defer t.lm.Unlock(futureKey)

	var setErr error
	if setErr = t.db.Set(ctx, key, val, opt.ToKV()); setErr != nil {
		exists, chkErr := t.checkVersion(ctx, key, types.NewKVReadCheckVersionOption(val.Version).CondTxnRecord(key != futureKey), t.maxRetry)
		if chkErr != nil {
			t.future.MustAdd(futureKey, val.Meta.ToDB().WithInvalidKeyState())
			if chkErr != errors.ErrDummy {
				glog.Errorf("[Transaction::SetRLocked] failed to check key '%s' on set error '%v' of version %d : '%v'", futureKey, setErr, val.Version, chkErr)
			}
			return errors.Annotatef(errors.ErrTabletTxnSetFailedKeyStatusUndetermined, "key: '%s', version: %d, set_error: '%v', get_err: '%v'", futureKey, val.Version, setErr, chkErr)
		}
		if !exists {
			glog.V(10).Infof("[Transaction::SetRLocked] check version %d exists for key '%s' on set error: '%v', and then found key not exists error", val.Version, futureKey, setErr)
			return errors.Annotatef(errors.ErrTabletTxnSetFailedKeyNotFound, "set_err: %v", setErr)
		}
		// assert.Must(checkErr == nil && exists) error pruning
	}
	if t.future.MustAdd(futureKey, val.Meta.ToDB()) && bool(glog.V(60)) {
		if setErr == nil {
			glog.Infof("[KVCC::%s][SetRLocked] added new key '%s' to txn-%d", trace.CallerFunc(), futureKey, t.ID)
		} else {
			glog.Infof("[KVCC::%s][SetRLocked] added new key '%s' to txn-%d, got set error '%v' but checked ok", trace.CallerFunc(), futureKey, t.ID, setErr)
		}
	}
	return nil
}

func (t *Transaction) checkVersion(ctx context.Context, key string, opt types.KVReadOption, maxRetry int) (exists bool, err error) {
	for i := 0; i < maxRetry && ctx.Err() == nil; i++ {
		var newVal types.Value
		if newVal, err = t.db.Get(ctx, key, opt); err == nil || errors.IsNotExistsErr(err) {
			assert.Must(newVal.Version == opt.Version)
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
	)
	if !opt.IsRollback() {
		keyStateAfterDone, action = types.KeyStateCommittedCleared, "clear txn record on commit"

		t.Lock() // NOTE: Lock is must though seems not needed
		t.setCommittedUnsafe(false, "clear txn record on commit by upper layer")
		t.Unlock()
	} else {
		// TODO maybe skip if txn record not written?
		keyStateAfterDone, action = types.KeyStateRollbackedCleared, "rollback txn record"

		t.Lock()
		t.SetAbortedUnsafe(false, "rollback txn record by upper layer")
		t.Unlock()
	}

	if err = removeTxnRecord(ctx, t.ID.Version(), action, t.db); err == nil {
		t.doneKey(t.ID.String(), keyStateAfterDone, opt.IsOperatedByDifferentTxn())
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

func (t *Transaction) doneKey(futureKey string, keyStateAfterDone types.KeyState, isOperatedByDifferentTxn bool) (doneOnce bool) {
	if isOperatedByDifferentTxn {
		return false
	}

	t.Lock()
	if doneOnce = t.future.DoneKey(futureKey, keyStateAfterDone); doneOnce {
		t.future.AssertAllKeysOfState(keyStateAfterDone)
		t.setTxnStateUnsafe(types.TxnState(keyStateAfterDone), true, "assert failed")
		t.Unlock()

		if glog.V(60) {
			glog.Infof("[Transaction::%s][doneKey] txn-%d done, state: %s, (all %d written keys include '%s' have been done)", trace.CallerFunc(), t.ID, t.GetTxnState(), t.future.GetAddedKeyCountUnsafe(), futureKey)
		}

		t.unref(t)
		return doneOnce
	}
	t.Unlock()

	return doneOnce
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

		t.future.NotifyTerminated(state, func(futureKey string, meta *types.DBMeta) {
			assert.Must(meta.IsKeyStateInvalid() && !assertValidAndTerminated)
			if state.IsAborted() {
				meta.ClearInvalidKeyState() // Allowed to clear positive invalid flag if is rollback, 1. prev exists-> version removed 2. prev not exists -> rollback is no op
				return
			}
			var (
				exists bool
				chkErr error
			)
			if meta.IsTxnRecord() {
				exists, chkErr = t.checkVersion(context.Background(), "", types.NewKVReadCheckVersionOption(t.ID.Version()).WithTxnRecord(), 100)
			} else {
				exists, chkErr = t.checkVersion(context.Background(), futureKey, types.NewKVReadCheckVersionOption(t.ID.Version()), 100)
			}
			if !exists || chkErr != nil {
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
