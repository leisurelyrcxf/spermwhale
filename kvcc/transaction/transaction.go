package transaction

import (
	"context"
	"fmt"
	"sync"

	"github.com/leisurelyrcxf/spermwhale/bench"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

type Transaction struct {
	sync.RWMutex

	ID types.TxnId
	types.TxnState

	writtenKeys concurrency.Future

	db types.KV

	rollbackedKey2Success map[string]bool

	keyEventWaitersMu sync.Mutex
	keyEventWaiters   map[string][]*KeyEventWaiter

	txnRecordMaxReadVersion uint64

	destroy func(*Transaction)
}

func newTransaction(id types.TxnId, db types.KV, destroy func(*Transaction)) *Transaction {
	return &Transaction{
		ID:       id,
		db:       db,
		TxnState: types.TxnStateUncommitted,
		destroy:  destroy,
	}
}

var invalidKeyWaiters = make([]*KeyEventWaiter, 0, 0)

func isInvalidKeyWaiters(waiters []*KeyEventWaiter) bool {
	return len(waiters) == 0 && waiters != nil
}

func (t *Transaction) DoneKey(ctx context.Context, key string, val types.Value, opt types.KVCCWriteOption) (err error) {
	var (
		txnId         = types.TxnId(val.Version)
		isRollbackKey = opt.IsRollbackKey()
		isWrittenKey  = !opt.IsReadModifyWriteRollbackOrClearReadKey() // clear or rollbacked written key
		doneOnce      bool
	)

	t.Lock()
	if opt.IsClearWriteIntent() {
		// NOTE: OK even if opt.IsReadModifyWriteRollbackOrClearReadKey() or kv.db.Set failed
		// TODO needs test against kv.db.Set() failed.
		t.signalKeyEventUnsafe(NewKeyEvent(key, KeyEventTypeClearWriteIntent))
	} else {
		assert.Must(isRollbackKey)
		assert.Must(!t.IsCommitted())
		if t.TxnState != types.TxnStateRollbacked {
			t.TxnState = types.TxnStateRollbacking
		}
	}

	if isWrittenKey {
		t.Unlock()
		if err = t.db.Set(ctx, key, val, opt.ToKVWriteOption()); err != nil && glog.V(10) {
			glog.Errorf("txn-%d set key '%s' failed: %v", txnId, key, err)
		}
		t.Lock()
	}
	if opt.IsClearWriteIntent() {
		if err == nil && glog.V(60) {
			glog.Infof("txn-%d cleared write intent of key '%s', cost: %s", txnId, key, bench.Elapsed())
		}
	} else {
		if err == nil && glog.V(60) {
			glog.Infof("txn-%d key '%s' rollbacked, cost: %s", txnId, key, bench.Elapsed())
		}
		t.signalKeyEventUnsafe(NewKeyEvent(key, GetKeyEventTypeRemoveVersion(err == nil)))
	}

	if err == nil && !opt.IsWriteByDifferentTransaction() {
		doneOnce = t.doneOnceUnsafe(key, isRollbackKey, "DoneKey")
	}
	t.Unlock()

	if doneOnce {
		t.GC()
	}
	return err
}

func (t *Transaction) GetTxnRecord(ctx context.Context, opt types.KVCCReadOption) (types.ValueCC, error) {
	t.RLock()
	defer t.RUnlock()

	if !opt.IsNotUpdateTimestampCache() {
		assert.Must(opt.ReaderVersion == types.MaxTxnVersion)
		t.txnRecordMaxReadVersion = opt.ReaderVersion
	}
	val, err := t.db.Get(ctx, "", opt.ToKVReadOption())
	return val.WithMaxReadVersion(t.txnRecordMaxReadVersion), err
}

func (t *Transaction) SetTxnRecord(ctx context.Context, val types.Value, opt types.KVCCWriteOption) error {
	t.Lock()
	defer t.Unlock()

	if t.TxnState != types.TxnStateUncommitted {
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

	txnKey := types.TxnId(val.Version).String()
	assert.Must(!t.writtenKeys.Done)
	inserted, keyDone := t.writtenKeys.AddUnsafe(txnKey)
	assert.Must(!keyDone)
	if inserted {
		glog.V(70).Infof("[KVCC::setKey] inserted key '%s' to txn-%d", txnKey, val.Version)
	}

	if err := t.db.Set(ctx, "", val, opt.ToKVWriteOption()); err != nil {
		if glog.V(4) {
			glog.Errorf("txn-%d set txn-record failed: '%v", val.Version, err)
		}
		return err
	}
	if glog.V(60) {
		glog.Infof("txn-%d set txn-record succeeded, cost %s", val.Version, bench.Elapsed())
	}
	return nil
}

func (t *Transaction) RemoveTxnRecord(ctx context.Context, val types.Value, opt types.KVCCWriteOption) (err error) {
	assert.Must(opt.IsRemoveVersion())
	t.Lock()
	if !opt.IsRollbackVersion() {
		assert.Must(!t.IsAborted())
		t.TxnState = types.TxnStateCommitted
	} else {
		assert.Must(!t.IsCommitted())
		if t.TxnState != types.TxnStateRollbacked {
			t.TxnState = types.TxnStateRollbacking
		}
	}
	t.Unlock()

	if err = t.db.Set(ctx, "", val, opt.ToKVWriteOption()); err != nil && glog.V(10) {
		glog.Errorf("txn-%d remove txn record failed: %v", val.Version, err)
	}

	var doneOnce bool
	if err == nil && !opt.IsWriteByDifferentTransaction() {
		t.Lock()
		doneOnce = t.doneOnceUnsafe(types.TxnId(val.Version).String(), opt.IsRollbackVersion(), "RemoveTxnRecord")
		t.Unlock()
	}

	if doneOnce {
		t.GC()
	}
	return err
}

func (t *Transaction) AddUnsafe(key string) (insertedNewKey bool, keyDone bool) {
	return t.writtenKeys.AddUnsafe(key)
}

func (t *Transaction) Done() bool {
	return t.writtenKeys.Done
}

func (t *Transaction) doneOnceUnsafe(key string, isRollback bool, caller string) (doneOnce bool) {
	if doneOnce = t.writtenKeys.DoneOnceUnsafe(key); doneOnce {
		glog.V(60).Infof("[Transaction::%s] txn-%d done (all %d written keys include '%s' have been done)", caller, t.ID, t.writtenKeys.GetAddedKeyCountUnsafe(), key)
		if isRollback {
			t.TxnState = types.TxnStateRollbacked
		} else {
			assert.Must(t.IsCommitted())
		}
	}
	return doneOnce
}

func (t *Transaction) GC() {
	t.destroy(t)
}

func (t *Transaction) registerKeyEventWaiter(key string) (*KeyEventWaiter, KeyEvent, error) {
	t.RLock()
	defer t.RUnlock()

	switch t.TxnState {
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
		panic(fmt.Sprintf("impossible state %s", t.TxnState))
	}
}

func (t *Transaction) signalKeyEventUnsafe(event KeyEvent) {
	if t.TxnState == types.TxnStateCommitted || t.TxnState == types.TxnStateRollbacked {
		return
	}
	assert.Must(!t.writtenKeys.Done)
	switch event.Type {
	case KeyEventTypeClearWriteIntent:
		t.TxnState = types.TxnStateCommitted
	case KeyEventTypeRemoveVersionFailed:
		t.TxnState = types.TxnStateRollbacking
		if t.rollbackedKey2Success == nil {
			t.rollbackedKey2Success = make(map[string]bool)
		}
		t.rollbackedKey2Success[event.Key] = false
	case KeyEventTypeVersionRemoved:
		t.TxnState = types.TxnStateRollbacking
		if t.rollbackedKey2Success == nil {
			t.rollbackedKey2Success = make(map[string]bool)
		}
		t.rollbackedKey2Success[event.Key] = true
	default:
		panic(fmt.Sprintf("invalid key event type: %s", event.Type))
	}

	t.keyEventWaitersMu.Lock()
	defer t.keyEventWaitersMu.Unlock()

	switch t.TxnState {
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
		panic(fmt.Sprintf("Transaction::signalKeyEventUnsafe: invalid Transaction state: %s", t.TxnState))
	}
	return
}

func (t *Transaction) getWaiterCounts() (waiterCount, waiterKeyCount int) {
	t.keyEventWaitersMu.Lock()
	defer t.keyEventWaitersMu.Unlock()

	for _, ws := range t.keyEventWaiters {
		waiterCount += len(ws)
	}
	return waiterCount, len(t.keyEventWaiters)
}
