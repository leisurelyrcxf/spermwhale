package transaction

import (
	"fmt"
	"sync"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

type transaction struct {
	sync.RWMutex

	id    types.TxnId
	state types.TxnState

	writtenKeyCount concurrency.AtomicUint64
	doneKeys        concurrency.ConcurrentSet

	keyEventWaitersMu sync.Mutex
	keyEventWaiters   map[string][]*KeyEventWaiter
}

func newTransaction(id types.TxnId) *transaction {
	t := &transaction{
		id:              id,
		state:           types.TxnStateUncommitted,
		keyEventWaiters: make(map[string][]*KeyEventWaiter),
	}
	t.doneKeys.Initialize()
	return t
}

func (t *transaction) GetState() types.TxnState {
	t.RLock()
	defer t.RUnlock()

	return t.state
}

var invalidKeyWaiters = make([]*KeyEventWaiter, 0, 0)

func isInvalidKeyWaiters(waiters []*KeyEventWaiter) bool {
	return len(waiters) == 0 && waiters != nil
}

func (t *transaction) addWrittenKey() {
	t.writtenKeyCount.Add(1)
}

func (t *transaction) doneKey(key string) bool {
	return t.doneKeys.Insert(key) >= int(t.writtenKeyCount.Get())
}

func (t *transaction) registerKeyEventWaiter(key string) (*KeyEventWaiter, KeyEvent, error) {
	t.RLock()
	defer t.RUnlock()

	switch t.state {
	case types.TxnStateCommitted:
		return nil, NewKeyEvent(key, KeyEventTypeClearWriteIntent), nil
	case types.TxnStateRollbacked:
		return nil, NewKeyEvent(key, KeyEventTypeVersionRemoved), nil
	case types.TxnStateRollbacking:
		if t.doneKeys.Contains(key) {
			return nil, NewKeyEvent(key, KeyEventTypeVersionRemoved), nil
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
		w := newkeyEventWaiter(key)
		t.keyEventWaiters[key] = append(t.keyEventWaiters[key], w)
		return w, InvalidKeyEvent, nil
	default:
		panic(fmt.Sprintf("impossible state %s", t.state))
	}
}

func (t *transaction) signal(event KeyEvent, checkDone bool) (shouldRemoveTxn bool) {
	t.Lock()
	if t.state == types.TxnStateCommitted || t.state == types.TxnStateRollbacked {
		t.Unlock()
		return false
	}
	switch event.Type {
	case KeyEventTypeClearWriteIntent:
		t.state = types.TxnStateCommitted
	case KeyEventTypeRemoveVersionFailed:
		t.state = types.TxnStateRollbacking
	case KeyEventTypeVersionRemoved:
		t.state = types.TxnStateRollbacking
		if t.doneKey(event.Key) /*NOTE: the order can't be changed */ && checkDone {
			t.state = types.TxnStateRollbacked
			shouldRemoveTxn = true
		}
	default:
		panic(fmt.Sprintf("invalid key event type: %s", event.Type))
	}
	t.Unlock()

	t.keyEventWaitersMu.Lock()
	defer t.keyEventWaitersMu.Unlock()

	switch t.state {
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
		panic(fmt.Sprintf("transaction::signal: invalid transaction state: %s", t.state))
	}
	return shouldRemoveTxn
}

func (t *transaction) getWaiterCounts() (waiterCount, waiterKeyCount int) {
	t.keyEventWaitersMu.Lock()
	defer t.keyEventWaitersMu.Unlock()

	for _, ws := range t.keyEventWaiters {
		waiterCount += len(ws)
	}
	return waiterCount, len(t.keyEventWaiters)
}
