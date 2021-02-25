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
	txns data_struct.ConcurrentMap

	kv             types.KV
	staleThreshold time.Duration
	oracle         *physical.Oracle
	store          *TransactionStore
	asyncJobs      chan Job
	workerNum      int
}

func NewTransactionManager(
	kv types.KV,
	staleThreshold time.Duration, workerNum int) *TransactionManager {
	return (&TransactionManager{
		kv:             kv,
		staleThreshold: staleThreshold,
		oracle:         physical.NewOracle(),
		asyncJobs:      make(chan Job, 1024),
		workerNum:      workerNum,
	}).createStore()
}

func (m *TransactionManager) Start() {
	for i := 0; i < m.workerNum; i++ {
		go func() {
			job, ok := <-m.asyncJobs
			if !ok {
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
			defer cancel()

			if err := job(ctx); err != nil {
				glog.Errorf("async job failed: %v", err)
			}
		}()
	}
}

func (m *TransactionManager) BeginTxn(_ context.Context) (*Txn, error) {
	id := m.oracle.MustFetchTimestamp()
	if _, ok := m.txns.Get(TransactionKey(id)); ok {
		return nil, errors.ErrTxnExists
	}
	txn := m.newTxn(id)
	m.txns.Set(TransactionKey(id), txn)
	return txn, nil
}

func (m *TransactionManager) newTxn(id uint64) *Txn {
	return NewTxn(id, m.kv, m.staleThreshold, m.oracle, m.store, m.asyncJobs)
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

func (m *TransactionManager) createStore() *TransactionManager {
	m.store = &TransactionStore{
		kv:             m.kv,
		oracle:         m.oracle,
		staleThreshold: m.staleThreshold,
		asyncJobs:      m.asyncJobs,
	}
	return m
}
