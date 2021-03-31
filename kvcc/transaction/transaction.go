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
	WrittenKeys concurrency.Future

	db types.KV

	rollbackedKey2Success map[string]bool

	keyEventWaitersMu sync.Mutex
	keyEventWaiters   map[string][]*KeyEventWaiter

	destroy func(*Transaction)
}

func newTransaction(id types.TxnId, db types.KV, destroy func(*Transaction)) *Transaction {
	t := &Transaction{
		ID:                    id,
		db:                    db,
		TxnState:              types.TxnStateUncommitted,
		keyEventWaiters:       make(map[string][]*KeyEventWaiter),
		rollbackedKey2Success: make(map[string]bool),
		destroy:               destroy,
	}
	t.WrittenKeys.Initialize()
	return t
}

var invalidKeyWaiters = make([]*KeyEventWaiter, 0, 0)

func isInvalidKeyWaiters(waiters []*KeyEventWaiter) bool {
	return len(waiters) == 0 && waiters != nil
}

func (t *Transaction) DoneKey(ctx context.Context, key string, val types.Value, opt types.KVCCWriteOption) (err error) {
	var (
		txnId        = types.TxnId(val.Version)
		isWrittenKey = !opt.IsReadModifyWriteRollbackOrClearReadKey() // clear or rollbacked written key
		doneOnce     bool
		checkDone    bool
	)

	t.Lock()
	if opt.IsClearWriteIntent() {
		// NOTE: OK even if opt.IsReadModifyWriteRollbackOrClearReadKey() or kv.db.Set failed
		// TODO needs test against kv.db.Set() failed.
		_ = t.signalKeyEventUnsafe(NewKeyEvent(key, KeyEventTypeClearWriteIntent), false)
	}

	if isWrittenKey {
		if err = t.db.Set(ctx, key, val, opt.ToKVWriteOption()); err != nil && glog.V(10) {
			glog.Errorf("txn-%d set key '%s' failed: %v", txnId, key, err)
		}
	}
	checkDone = err == nil && !opt.IsWriteByDifferentTransaction() && !t.WrittenKeys.Done
	if opt.IsClearWriteIntent() {
		if err == nil && glog.V(60) {
			glog.Infof("txn-%d clear write intent of key '%s' cleared, cost: %s", txnId, key, bench.Elapsed())
		}
		if checkDone {
			assert.Must(isWrittenKey)
			if doneOnce = t.WrittenKeys.DoneUnsafe(key); doneOnce && bool(glog.V(60)) {
				glog.Infof("[KVCC::clearOrRemoveKey] txn-%d commit-cleared (all %d written keys include '%s' have been cleared)", txnId, t.WrittenKeys.GetAddedKeyCountUnsafe(), key)
			}
		}
	} else if opt.IsRollbackKey() {
		if err == nil && glog.V(60) {
			glog.Infof("txn-%d key '%s' rollbacked, cost: %s", txnId, key, bench.Elapsed())
		}
		if doneOnce = t.signalKeyEventUnsafe(NewKeyEvent(key,
			GetKeyEventTypeRemoveVersion(err == nil)), checkDone); doneOnce && bool(glog.V(60)) {
			glog.Infof("[KVCC::clearOrRemoveKey] txn-%d rollbacked (all %d written keys include '%s' have been removed)", txnId, t.WrittenKeys.GetAddedKeyCountUnsafe(), key)
		}
	}
	t.Unlock()

	if doneOnce {
		t.GC()
	}
	return err
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
		t.keyEventWaiters[key] = append(t.keyEventWaiters[key], w)
		return w, InvalidKeyEvent, nil
	default:
		panic(fmt.Sprintf("impossible state %s", t.TxnState))
	}
}

func (t *Transaction) signalKeyEventUnsafe(event KeyEvent, checkDone bool) (doneOnce bool) {
	if t.TxnState == types.TxnStateCommitted || t.TxnState == types.TxnStateRollbacked {
		return false
	}
	assert.Must(!t.WrittenKeys.Done)
	switch event.Type {
	case KeyEventTypeClearWriteIntent:
		t.TxnState = types.TxnStateCommitted
	case KeyEventTypeRemoveVersionFailed:
		t.TxnState = types.TxnStateRollbacking
		t.rollbackedKey2Success[event.Key] = false
	case KeyEventTypeVersionRemoved:
		t.TxnState = types.TxnStateRollbacking
		t.rollbackedKey2Success[event.Key] = true
		if checkDone && t.WrittenKeys.DoneUnsafe(event.Key) {
			t.TxnState = types.TxnStateRollbacked
			doneOnce = true
		}
	default:
		panic(fmt.Sprintf("invalid key event type: %s", event.Type))
	}

	t.keyEventWaitersMu.Lock()
	defer t.keyEventWaitersMu.Unlock()

	switch t.TxnState {
	case types.TxnStateCommitted, types.TxnStateRollbacked:
		for key, ws := range t.keyEventWaiters {
			for _, w := range ws {
				w.signal(NewKeyEvent(key, event.Type))
			}
			t.keyEventWaiters[key] = invalidKeyWaiters // once fired, should no longer register again
		}
	case types.TxnStateRollbacking:
		for _, w := range t.keyEventWaiters[event.Key] {
			w.signal(event)
		}
		t.keyEventWaiters[event.Key] = invalidKeyWaiters // once fired, should no longer register again
	default:
		panic(fmt.Sprintf("Transaction::signalKeyEventUnsafe: invalid Transaction state: %s", t.TxnState))
	}
	return doneOnce
}

func (t *Transaction) getWaiterCounts() (waiterCount, waiterKeyCount int) {
	t.keyEventWaitersMu.Lock()
	defer t.keyEventWaitersMu.Unlock()

	for _, ws := range t.keyEventWaiters {
		waiterCount += len(ws)
	}
	return waiterCount, len(t.keyEventWaiters)
}
