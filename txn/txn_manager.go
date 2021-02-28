package txn

import (
	"context"
	"sync/atomic"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/oracle/impl"

	"github.com/leisurelyrcxf/spermwhale/topo"

	"github.com/leisurelyrcxf/spermwhale/utils"

	"github.com/leisurelyrcxf/spermwhale/oracle"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"
	scheduler "github.com/leisurelyrcxf/spermwhale/scheduler"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

const MaxTaskBuffered = 10000

type Scheduler struct {
	clearJobScheduler *scheduler.BasicScheduler
	ioJobScheduler    *scheduler.ConcurrentListScheduler
}

func (s *Scheduler) ScheduleClearJob(t *types.Task) error {
	return s.clearJobScheduler.Schedule(t)
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
	cfg types.TxnConfig

	txns concurrency.ConcurrentTxnMap

	kv        types.KV
	oracle    atomic.Value
	store     *TransactionStore
	topoStore *topo.Store

	s *Scheduler
}

func NewTransactionManager(
	kv types.KV,
	cfg types.TxnConfig,
	clearWorkerNum, ioWorkerNum int) *TransactionManager {
	return NewTransactionManagerWithOracle(kv, cfg, clearWorkerNum, ioWorkerNum, physical.NewOracle())
}

func NewTransactionManagerWithCluster(
	kv types.KV,
	cfg types.TxnConfig,
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
	kv types.KV,
	cfg types.TxnConfig,
	clearWorkerNum, ioWorkerNum int,
	oracle oracle.Oracle) *TransactionManager {
	tm := (&TransactionManager{
		kv:  kv,
		cfg: cfg,

		s: &Scheduler{
			clearJobScheduler: scheduler.NewBasicScheduler(MaxTaskBuffered, clearWorkerNum),
			ioJobScheduler:    scheduler.NewConcurrentListScheduler(MaxTaskBuffered, ioWorkerNum, 1),
		},
	}).createStore()
	tm.txns.Initialize(32)
	if oracle != nil {
		tm.oracle.Store(oracle)
	}
	return tm
}

func (m *TransactionManager) BeginTransaction(_ context.Context) (types.Txn, error) {
	ts, err := utils.FetchTimestampWithRetry(m)
	if err != nil {
		return nil, err
	}
	txnID := types.TxnId(ts)
	if _, ok := m.txns.Get(txnID); ok {
		return nil, errors.ErrTxnExists
	}
	txn := m.newTxn(txnID)
	m.txns.SetIf(txnID, txn, func(prev interface{}, exist bool) bool {
		assert.Must(!exist)
		return !exist
	})
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
	m.txns.Clear()
	return m.kv.Close()
}

func (m *TransactionManager) newTxn(id types.TxnId) *Txn {
	return NewTxn(id, m.kv, m.cfg, m.store, m, m.s)
}

func (m *TransactionManager) createStore() *TransactionManager {
	m.store = &TransactionStore{
		kv:  m.kv,
		cfg: m.cfg,

		txnInitializer: func(txn *Txn) {
			txn.lastWriteKeyTasks = make(map[string]*types.ListTask)
			txn.cfg = m.cfg
			txn.kv = m.kv
			txn.store = m.store
			txn.s = m.s
			txn.h = m
		},
		txnConstructor: func(txnId types.TxnId) *Txn {
			return m.newTxn(txnId)
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
	return m.oracle.Load().(oracle.Oracle)
}

func (m *TransactionManager) watchOracle() error {
	watchFuture, err := m.topoStore.Client().WatchOnce(m.topoStore.OraclePath())
	if err != nil && !errors.IsNotSupportedErr(err) {
		return err
	}

	if errors.IsNotSupportedErr(err) {
		if m.oracle.Load() != nil {
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
