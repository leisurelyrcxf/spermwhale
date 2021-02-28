package txn

import (
	"context"

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
	oracle    *physical.Oracle
	store     *TransactionStore
	workerNum int

	s *Scheduler
}

func NewTransactionManager(
	kv types.KV,
	cfg types.TxnConfig,
	clearWorkerNum, ioWorkerNum int) *TransactionManager {
	tm := (&TransactionManager{
		txns: concurrency.NewConcurrentTxnMap(32),

		kv:        kv,
		cfg:       cfg,
		oracle:    physical.NewOracle(),
		workerNum: clearWorkerNum,

		s: &Scheduler{
			clearJobScheduler: scheduler.NewBasicScheduler(MaxTaskBuffered, clearWorkerNum),
			ioJobScheduler:    scheduler.NewConcurrentListScheduler(ioWorkerNum, MaxTaskBuffered, 1),
		},
	}).createStore()
	return tm
}

func (m *TransactionManager) BeginTransaction(_ context.Context) (types.Txn, error) {
	txnID := types.TxnId(m.oracle.MustFetchTimestamp())
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
