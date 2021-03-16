package transaction

import (
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/scheduler"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

const (
	EstimatedMaxQPS        = 1000000
	RemoveTxnDelay         = time.Millisecond * 200
	EstimateMaxBufferedTxn = int(EstimatedMaxQPS * (float64(RemoveTxnDelay) / float64(time.Second)))
	TimerPartitionNum      = 8
	TimerPartitionChSize   = EstimateMaxBufferedTxn / TimerPartitionNum
)

type Manager struct {
	writeTxns          concurrency.ConcurrentTxnMap
	readForWriteQueues concurrency.ConcurrentMap
	timer              scheduler.ConcurrentBasicTimer
}

func NewManager() *Manager {
	tm := &Manager{}
	tm.writeTxns.Initialize(64)
	tm.readForWriteQueues.Initialize(64)
	tm.timer.Initialize(TimerPartitionNum, utils.MaxInt(TimerPartitionChSize, 100))
	tm.timer.Start()
	return tm
}

func (tm *Manager) GetTxnState(txnId types.TxnId) types.TxnState {
	txn := tm.getTxn(txnId)
	if txn == nil {
		return types.TxnStateInvalid
	}
	return txn.GetState()
}

func (tm *Manager) PushReadForWriteReaderOnKey(key string, readerTxnId types.TxnId) (*readForWriteCond, error) {
	return tm.readForWriteQueues.GetLazy(key, func() interface{} {
		return newReadForWriteQueue(key)
	}).(*readForWriteQueue).pushReader(readerTxnId)
}

func (tm *Manager) NotifyReadForWriteKeyDone(key string, readForWriteTxnId types.TxnId) {
	pq, ok := tm.readForWriteQueues.Get(key)
	if !ok {
		return
	}
	pq.(*readForWriteQueue).notifyKeyDone(readForWriteTxnId)
}

func (tm *Manager) AddWriteTransactionWrittenKey(id types.TxnId) {
	tm.writeTxns.GetLazy(id, func() interface{} {
		return newTransaction(id)
	}).(*transaction).addWrittenKey()
}

func (tm *Manager) RegisterKeyEventWaiter(waitForWriteTxnId types.TxnId, key string) (*KeyEventWaiter, KeyEvent, error) {
	waitFor := tm.getTxn(waitForWriteTxnId)
	if waitFor == nil {
		assert.Must(false) // TODO remove in product
		return nil, InvalidKeyEvent, errors.Annotatef(errors.ErrTabletWriteTransactionNotFound, "key: %s", key)
	}
	return waitFor.registerKeyEventWaiter(key)
}

func (tm *Manager) SignalKeyEvent(writeTxnId types.TxnId, event KeyEvent, checkDone bool) {
	txn := tm.getTxn(writeTxnId)
	if txn == nil {
		return
	}
	if txn.signalKeyEvent(event, checkDone) {
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
	assert.Must(txn.GetState().IsTerminated())
	waiterCount, waiterKeyCount := txn.getWaiterCounts()
	assert.Must(waiterCount == 0)
	assert.Must(waiterKeyCount <= int(txn.writtenKeyCount.Get()))

	tm.timer.Schedule(
		scheduler.NewTimerTask(
			time.Now().Add(RemoveTxnDelay), func() {
				tm.writeTxns.Del(txn.id)
			},
		),
	)
}

func (tm *Manager) Close() {
	tm.writeTxns.Close()
	tm.readForWriteQueues.Clear()
	tm.timer.Close()
}
