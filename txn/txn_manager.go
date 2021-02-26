package txn

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/assert"

	"github.com/leisurelyrcxf/spermwhale/data_struct"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"
	"github.com/leisurelyrcxf/spermwhale/types"
)

const MaxTaskBuffered = 1024

type Scheduler struct {
	clearJobScheduler *types.ListScheduler
	ioJobScheduler    *types.ListScheduler
}

func (s *Scheduler) ScheduleClearJob(t *types.Task) error {
	return s.clearJobScheduler.Schedule(t)
}

func (s *Scheduler) ScheduleIOJob(t *types.Task) error {
	return s.ioJobScheduler.Schedule(t)
}

type TransactionManager struct {
	types.TxnConfig

	txns data_struct.ConcurrentMap

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
		txns: data_struct.NewConcurrentMap(32),

		kv:        kv,
		TxnConfig: cfg,
		oracle:    physical.NewOracle(),
		workerNum: clearWorkerNum,

		s: &Scheduler{
			clearJobScheduler: types.NewListScheduler(MaxTaskBuffered, clearWorkerNum),
			ioJobScheduler:    types.NewListScheduler(MaxTaskBuffered, ioWorkerNum),
		},
	}).createStore()
	return tm
}

func (m *TransactionManager) BeginTransaction(_ context.Context) (types.Txn, error) {
	id := m.oracle.MustFetchTimestamp()
	if _, ok := m.txns.Get(TransactionKey(id)); ok {
		return nil, errors.ErrTxnExists
	}
	txn := m.newTxn(id)
	m.txns.SetIf(TransactionKey(id), txn, func(prev interface{}, exist bool) bool {
		assert.Must(!exist)
		return !exist
	})
	return txn, nil
}

func (m *TransactionManager) GetTxn(id uint64) (*Txn, error) {
	if txnVal, ok := m.txns.Get(TransactionKey(id)); ok {
		return txnVal.(*Txn), nil
	}
	return nil, errors.ErrTransactionNotFound
}

func (m *TransactionManager) Close() error {
	return m.kv.Close()
}

func (m *TransactionManager) newTxn(id uint64) *Txn {
	return NewTxn(id, m.kv, m.TxnConfig, m.oracle, m.store, m.s)
}

func (m *TransactionManager) createStore() *TransactionManager {
	m.store = &TransactionStore{
		kv:     m.kv,
		oracle: m.oracle,
		cfg:    m.TxnConfig,
		s:      m.s,
	}
	return m
}
