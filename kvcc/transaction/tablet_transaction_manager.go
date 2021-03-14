package transaction

import (
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

const EstimatedMaxQPS = 100000

type Manager struct {
	writeTxns concurrency.ConcurrentTxnMap
}

func NewManager(_ time.Duration) *Manager {
	tm := &Manager{}
	tm.writeTxns.Initialize(64)
	return tm
}

func (tm *Manager) GetTxnState(txnId types.TxnId) types.TxnState {
	txn := tm.getTxn(txnId)
	if txn == nil {
		return types.TxnStateInvalid
	}
	return txn.GetState()
}

func (tm *Manager) AddWriteTransactionWrittenKey(id types.TxnId) {
	tm.writeTxns.GetLazy(id, func() interface{} {
		return newTransaction(id)
	}).(*transaction).addWrittenKey()
}

func (tm *Manager) RegisterKeyEventWaiter(waitForWriteTxnId types.TxnId, key string) (*KeyEventWaiter, KeyEvent, error) {
	waitFor := tm.getTxn(waitForWriteTxnId)
	if waitFor == nil {
		return nil, InvalidKeyEvent, errors.Annotatef(errors.ErrTabletWriteTransactionNotFound, "key: %s", key)
	}
	return waitFor.registerKeyEventWaiter(key)
}

func (tm *Manager) Signal(writeTxnId types.TxnId, event KeyEvent, checkDone bool) {
	txn := tm.getTxn(writeTxnId)
	if txn == nil {
		return
	}
	if txn.signal(event, checkDone) {
		tm.removeTxn(txn)
	}
}

func (tm *Manager) DoneKey(txnId types.TxnId, key string) {
	txn := tm.getTxn(txnId)
	if txn == nil {
		return
	}
	if txn.doneKey(key) {
		tm.removeTxn(txn)
	}
}

func (tm *Manager) getTxn(txnId types.TxnId) *transaction {
	i, ok := tm.writeTxns.Get(txnId)
	if !ok {
		return nil
	}
	return i.(*transaction)
}

func (tm *Manager) removeTxn(txn *transaction) {
	// TODO remove this in product
	//assert.Must(txn.writtenKeyCount.Get() == 2)
	assert.Must(txn.GetState().IsTerminated())
	waiterCount, waiterKeyCount := txn.getWaiterCounts()
	assert.Must(waiterCount == 0)
	assert.Must(waiterKeyCount <= int(txn.writtenKeyCount.Get()))

	tm.writeTxns.Del(txn.id)
}

func (tm *Manager) Close() {
	tm.writeTxns.Close()
}
