package transaction

import (
	"context"
	"time"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

const (
	EstimatedMaxQPS = 1000000
	TxnPartitionNum = 64

	TabletTransactionVerboseLevel = 150
)

type Manager struct {
	writeTxns             concurrency.ConcurrentTxnMap
	readModifyWriteQueues concurrency.ConcurrentMap
	db                    types.KV

	cfg types.TabletTxnManagerConfig
}

func NewManager(cfg types.TabletTxnManagerConfig, db types.KV) *Manager {
	tm := &Manager{cfg: cfg, db: db}
	estimateMaxBufferedTxn := int(EstimatedMaxQPS * (float64(tm.cfg.TxnLifeSpan) / float64(time.Second)))
	tm.writeTxns.InitializeWithGCThreads(TxnPartitionNum, utils.MaxInt(estimateMaxBufferedTxn/TxnPartitionNum, 100), cfg.MinGCThreadMinInterrupt)
	tm.readModifyWriteQueues.Initialize(64)
	return tm
}

func (tm *Manager) newTransaction(id types.TxnId) *Transaction {
	return newTransaction(id, tm.db, func(transaction *Transaction) {
		tm.removeWhen(transaction)
		transaction.Unreffed.Set(true)
	})
}

func (tm *Manager) PushReadModifyWriteReaderOnKey(key string, readOpt types.KVCCReadOption) (*readModifyWriteCond, error) {
	return tm.readModifyWriteQueues.GetLazy(key, func() interface{} {
		return newReadModifyWriteQueue(key, tm.cfg.StaleWriteThreshold, tm.cfg.ReadModifyWriteQueueCfg)
	}).(*readModifyWriteQueue).pushReader(readOpt)
}

func (tm *Manager) SignalReadModifyWriteKeyEvent(readModifyWriteTxnId types.TxnId, event ReadModifyWriteKeyEvent) {
	pq, ok := tm.readModifyWriteQueues.Get(event.Key)
	if !ok {
		return
	}
	pq.(*readModifyWriteQueue).notifyKeyEvent(readModifyWriteTxnId, event.Type)
}

func (tm *Manager) InsertTxnIfNotExists(id types.TxnId) (inserted bool, txn *Transaction, err error) {
	inserted, obj := tm.writeTxns.InsertIfNotExists(id, func() interface{} {
		if utils.IsTooOld(id.Version(), tm.cfg.TxnInsertThreshold) { // guarantee no txn inserted after txn removed from Manager
			return nil
		}
		return tm.newTransaction(id)
	})
	if obj == nil {
		return false, nil, errors.ErrStaleWriteInsertTooOldTxn
	}
	return inserted, obj.(*Transaction), nil
}

func (tm *Manager) ClearWriteIntent(ctx context.Context, key string, version uint64, opt types.KVCCUpdateMetaOption) error {
	txn, getErr := tm.GetTxn(types.TxnId(version))
	if getErr != nil {
		glog.V(4).Infof("[Manager::ClearWriteIntent] can't get txn-%d", version)
		_, err := clearWriteIntent(ctx, key, version, opt, tm.db)
		return err
	}
	return txn.ClearWriteIntent(ctx, key, opt)
}

func (tm *Manager) RollbackKey(ctx context.Context, key string, version uint64, opt types.KVCCRollbackKeyOption) error {
	inserted, txn, insertErr := tm.InsertTxnIfNotExists(types.TxnId(version)) // TODO not insert if too stale
	if insertErr != nil {
		glog.V(4).Infof("[Manager::RollbackKey] failed to insert txn-%d", version)
		_, err := rollbackKey(ctx, key, version, opt, tm.db)
		return err
	}
	if inserted {
		glog.V(TabletTransactionVerboseLevel).Infof("[Manager::RollbackKey] created new txn-%d", version)
	}
	return txn.RollbackKey(ctx, key, opt)
}

func (tm *Manager) RemoveTxnRecord(ctx context.Context, version uint64, opt types.KVCCRemoveTxnRecordOption) error {
	var (
		txn *Transaction
		err error
	)
	if !opt.IsRollback() {
		if txn, err = tm.GetTxn(types.TxnId(version)); err != nil {
			return removeTxnRecord(ctx, version, "clear txn record on commit", tm.db)
		}
	} else {
		var inserted bool
		if inserted, txn, err = tm.InsertTxnIfNotExists(types.TxnId(version)); err != nil {
			glog.V(6).Infof("[Manager::RemoveTxnRecord] failed to insert txn-%d", version)
			return removeTxnRecord(ctx, version, "rollback txn record", tm.db)
		}
		if inserted {
			glog.V(TabletTransactionVerboseLevel).Infof("[Manager::RemoveTxnRecord] created new txn-%d", version)
		}
	}
	return txn.RemoveTxnRecord(ctx, opt)
}

func (tm *Manager) GetTxn(txnId types.TxnId) (*Transaction, error) {
	i, ok := tm.writeTxns.Get(txnId)
	if !ok {
		return nil, errors.ErrTabletWriteTransactionNotFound
	}
	return i.(*Transaction), nil
}

func (tm *Manager) GetWriteTxns() *concurrency.ConcurrentTxnMap {
	return &tm.writeTxns
}

func (tm *Manager) removeWhen(txn *Transaction) {
	assert.Must(txn.IsTerminated())
	tm.writeTxns.RemoveWhen(txn.ID, txn.ID.After(tm.cfg.TxnLifeSpan))
}

func (tm *Manager) Close() {
	tm.readModifyWriteQueues.Clear()
	tm.writeTxns.Close()
}
