package txn

import (
	"context"
	"sync/atomic"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/oracle"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"
	"github.com/leisurelyrcxf/spermwhale/scheduler"
	"github.com/leisurelyrcxf/spermwhale/topo"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

const MaxTaskBufferedPerPartition = 10000

type Scheduler struct {
	clearJobScheduler *scheduler.ConcurrentStaticTreeScheduler
	ioJobScheduler    *scheduler.ConcurrentDynamicListScheduler
}

func (s *Scheduler) ScheduleClearJobTree(root *types.TreeTask) error {
	return s.clearJobScheduler.ScheduleTree(root)
}

func (s *Scheduler) ScheduleIOJob(t *types.ListTask) error {
	return s.ioJobScheduler.Schedule(t)
}

func (s *Scheduler) GCIOJobs(ts []*types.ListTask) {
	s.ioJobScheduler.GC(ts)
}

func (s *Scheduler) Close() {
	s.clearJobScheduler.Close()
	s.ioJobScheduler.Close()
}

type TransactionManager struct {
	cfg types.TxnManagerConfig

	txns concurrency.ConcurrentTxnMap

	kv        types.KVCC
	oracle    atomic.Value
	store     *TransactionStore
	topoStore *topo.Store

	s *Scheduler
}

func NewTransactionManager(
	kv types.KVCC,
	cfg types.TxnManagerConfig,
	clearWorkerNum, ioWorkerNum int) *TransactionManager {
	return NewTransactionManagerWithOracle(kv, cfg, clearWorkerNum, ioWorkerNum, physical.NewOracle())
}

func NewTransactionManagerWithCluster(
	kv types.KVCC,
	cfg types.TxnManagerConfig,
	clearWorkerNum, ioWorkerNum int,
	store *topo.Store) (*TransactionManager, error) {
	tm := NewTransactionManagerWithOracle(kv, cfg, clearWorkerNum, ioWorkerNum, nil)
	tm.topoStore = store
	if err := tm.syncOracle(); err != nil {
		glog.Warningf("can't initialize oracle: %v", err)
	}
	if err := tm.watchOracle(); err != nil {
		return nil, err
	}
	return tm, nil
}

func NewTransactionManagerWithOracle(
	kv types.KVCC,
	cfg types.TxnManagerConfig,
	clearWorkerNum, ioWorkerNum int,
	oracle oracle.Oracle) *TransactionManager {
	tm := (&TransactionManager{
		kv:  kv,
		cfg: cfg,

		s: &Scheduler{
			clearJobScheduler: scheduler.NewConcurrentStaticTreeScheduler(clearWorkerNum, MaxTaskBufferedPerPartition, 1),
			ioJobScheduler:    scheduler.NewConcurrentDynamicListScheduler(ioWorkerNum, MaxTaskBufferedPerPartition, 1),
		},
	}).createStore()
	tm.txns.Initialize(32)
	if oracle != nil {
		tm.oracle.Store(oracle)
	}
	return tm
}

func (m *TransactionManager) BeginTransaction(_ context.Context, typ types.TxnType) (types.Txn, error) {
	ts, err := utils.FetchTimestampWithRetry(m)
	if err != nil {
		return nil, err
	}
	txnID := types.TxnId(ts)
	if _, ok := m.txns.Get(txnID); ok {
		return nil, errors.ErrTxnExists
	}
	txn := m.newTxn(txnID, typ)
	err = m.txns.Insert(txnID, txn)
	assert.MustNoError(err)
	return txn, nil
}

func (m *TransactionManager) GetTxn(txnID types.TxnId) (*Txn, error) {
	if txnVal, ok := m.txns.Get(txnID); ok {
		return txnVal.(*Txn), nil
	}
	return nil, errors.ErrTransactionNotFound
}

func (m *TransactionManager) RemoveTxn(txn *Txn) {
	m.txns.Del(txn.ID)
}

func (m *TransactionManager) Close() error {
	m.s.Close()
	m.txns.Close()
	return m.kv.Close()
}

func (m *TransactionManager) newTxn(id types.TxnId, typ types.TxnType) *Txn {
	return NewTxn(id, typ, m.kv, m.cfg, m.store, m, m.s)
}

func (m *TransactionManager) createStore() *TransactionManager {
	m.store = &TransactionStore{
		kv:              m.kv,
		cfg:             m.cfg,
		retryWaitPeriod: consts.DefaultRetryWaitPeriod,

		txnInitializer: func(record *Txn) {
			record.lastWriteKeyTasks = make(map[string]*types.ListTask)
			for _, key := range record.WrittenKeys {
				record.lastWriteKeyTasks[key] = nil
			}
			record.cfg = m.cfg
			record.kv = m.kv
			record.store = m.store
			record.s = m.s
			record.h = m
		},
		txnConstructor: func(txnId types.TxnId, state types.TxnState, writtenKeys []string) *Txn {
			txn := m.newTxn(txnId, types.TxnTypeDefault)
			txn.State = state
			txn.WrittenKeys = writtenKeys
			for _, key := range txn.WrittenKeys {
				txn.lastWriteKeyTasks[key] = nil
			}
			return txn
		},
	}
	return m
}

func (m *TransactionManager) syncOracle() error {
	o, err := m.topoStore.LoadOracle()
	if err != nil {
		glog.Errorf("synchronized oracle failed, can't load from store: '%v'", err)
		return err
	}
	cli, err := impl.NewClient(o.ServerAddr)
	if err != nil {
		glog.Errorf("synchronized oracle to %s failed during creating client: '%v'", o.ServerAddr, err)
		return err
	}
	m.oracle.Store(cli)
	glog.Infof("synchronized oracle to %s successfully", o.ServerAddr)
	return nil
}

func (m *TransactionManager) GetOracle() oracle.Oracle {
	if v, ok := m.oracle.Load().(oracle.Oracle); ok {
		return v
	}
	return nil
}

func (m *TransactionManager) watchOracle() error {
	watchFuture, err := m.topoStore.Client().WatchOnce(m.topoStore.OraclePath())
	if err != nil && !errors.IsNotSupportedErr(err) {
		return err
	}

	if errors.IsNotSupportedErr(err) {
		if m.GetOracle() != nil {
			return nil
		}
		return err
	}

	go func() {
		for {
			<-watchFuture

			if err := m.syncOracle(); err != nil {
				glog.Fatalf("sync oracle failed: '%v'", err)
			}
			var err error
			if watchFuture, err = m.topoStore.Client().WatchOnce(m.topoStore.OraclePath()); err != nil {
				glog.Fatalf("watch once failed: '%v'", err)
			}
		}
	}()
	return nil
}
