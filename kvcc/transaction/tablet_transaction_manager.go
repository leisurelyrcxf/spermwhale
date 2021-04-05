package transaction

import (
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

const (
	EstimatedMaxQPS = 1000000
	TxnPartitionNum = 256
)

type Manager struct {
	writeTxns             concurrency.ConcurrentTxnMap
	readModifyWriteQueues concurrency.ConcurrentMap

	cfg types.TabletTxnManagerConfig
}

func NewManager(cfg types.TabletTxnManagerConfig) *Manager {
	tm := &Manager{cfg: cfg}

	estimateMaxBufferedTxn := int(EstimatedMaxQPS * (float64(tm.cfg.TxnLifeSpan) / float64(time.Second)))
	tm.writeTxns.InitializeWithGCThreads(TxnPartitionNum, utils.MaxInt(estimateMaxBufferedTxn/TxnPartitionNum, 100))
	tm.readModifyWriteQueues.Initialize(64)
	return tm
}

func (tm *Manager) PushReadModifyWriteReaderOnKey(key string, readOpt types.KVCCReadOption) (*readModifyWriteCond, error) {
	return tm.readModifyWriteQueues.GetLazy(key, func() interface{} {
		return newReadModifyWriteQueue(key, tm.cfg.ReadModifyWriteQueueCfg)
	}).(*readModifyWriteQueue).pushReader(readOpt)
}

func (tm *Manager) SignalReadModifyWriteKeyEvent(readModifyWriteTxnId types.TxnId, event ReadModifyWriteKeyEvent) {
	pq, ok := tm.readModifyWriteQueues.Get(event.Key)
	if !ok {
		return
	}
	pq.(*readModifyWriteQueue).notifyKeyEvent(readModifyWriteTxnId, event.Type)
}

func (tm *Manager) MustInsertTxnIfNotExists(id types.TxnId, db types.KV) (inserted bool, txn *Transaction) {
	inserted, obj := tm.writeTxns.InsertIfNotExists(id, func() interface{} {
		return newTransaction(id, db, func(transaction *Transaction) {
			tm.removeTxn(transaction)
		})
	})
	return inserted, obj.(*Transaction)
}

func (tm *Manager) InsertTxnIfNotExists(id types.TxnId, db types.KV) (inserted bool, txn *Transaction, err error) {
	inserted, obj := tm.writeTxns.InsertIfNotExists(id, func() interface{} {
		if utils.IsTooOld(id.Version(), tm.cfg.StaleWriteThreshold) { // guarantee no txn inserted after Manager::removeTxn() was called
			return nil
		}
		return newTransaction(id, db, func(transaction *Transaction) {
			tm.removeTxn(transaction)
		})
	})
	if obj == nil {
		return false, nil, errors.ErrStaleWriteInsertTooOldTxn
	}
	return inserted, obj.(*Transaction), nil
}

func (tm *Manager) GetTxn(txnId types.TxnId) (*Transaction, error) {
	i, ok := tm.writeTxns.Get(txnId)
	if !ok {
		return nil, errors.ErrTabletWriteTransactionNotFound
	}
	return i.(*Transaction), nil
}

func (tm *Manager) removeTxn(txn *Transaction) {
	assert.Must(txn.IsTerminated())
	tm.writeTxns.GCWhen(txn.ID, txn.ID.After(tm.cfg.TxnLifeSpan))
}

func (tm *Manager) Close() {
	tm.readModifyWriteQueues.Clear()
	tm.writeTxns.Close()
}
