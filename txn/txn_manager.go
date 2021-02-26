package txn

import (
	"context"
	"time"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/data_struct"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"
	"github.com/leisurelyrcxf/spermwhale/types"
)

type TransactionManager struct {
	types.TxnConfig

	txns data_struct.ConcurrentMap

	kv        types.KV
	oracle    *physical.Oracle
	store     *TransactionStore
	asyncJobs chan Job
	workerNum int
}

func NewTransactionManager(
	kv types.KV,
	cfg types.TxnConfig,
	workerNum int) *TransactionManager {
	tm := (&TransactionManager{
		txns: data_struct.NewConcurrentMap(32),

		kv:        kv,
		TxnConfig: cfg,
		oracle:    physical.NewOracle(),
		asyncJobs: make(chan Job, 1024),
		workerNum: workerNum,
	}).createStore()
	tm.Start()
	return tm
}

func (m *TransactionManager) Start() {
	for i := 0; i < m.workerNum; i++ {
		go func() {
			for {
				job, ok := <-m.asyncJobs
				if !ok {
					return
				}

				ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
				if err := job(ctx); err != nil {
					glog.Errorf("async job failed: %v", err)
				}
				cancel()
			}
		}()
	}
}

func (m *TransactionManager) BeginTransaction(_ context.Context) (types.Txn, error) {
	id := m.oracle.MustFetchTimestamp()
	if _, ok := m.txns.Get(TransactionKey(id)); ok {
		return nil, errors.ErrTxnExists
	}
	txn := m.newTxn(id)
	m.txns.Set(TransactionKey(id), txn)
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
	return NewTxn(id, m.kv, m.TxnConfig, m.oracle, m.store, m.asyncJobs)
}

func (m *TransactionManager) createStore() *TransactionManager {
	m.store = &TransactionStore{
		kv:        m.kv,
		oracle:    m.oracle,
		cfg:       m.TxnConfig,
		asyncJobs: m.asyncJobs,
	}
	return m
}
