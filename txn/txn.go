package txn

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/leisurelyrcxf/spermwhale/types"
)

type State string

const (
	StateUnknown     = ""
	StateUncommitted = "uncommitted"
	StateStaging     = "staging"
	StateCommitted   = "committed"
)

type Txn struct {
	ID uint64
	kv types.KV

	tm    *TransactionManager
	state State
}

func NewTransaction(id uint64, kv types.KV) *Txn {
	return &Txn{
		ID: id,
		kv: kv,
	}
}

func (txn *Txn) IsCommitted() bool {
	return txn.state == StateCommitted
}

func (txn *Txn) Get(ctx context.Context, key string) (string, error) {
	vv, err := txn.kv.Get(ctx, key, txn.ID)
	if err != nil {
		return "", err
	}
	assert.Must(vv.Version <= txn.ID)
	if !vv.M.WriteIntent {
		return vv.V, nil
	}
	writeTxn, err := txn.tm.GetTxn(vv.Version)
	if err != nil {
		return "", err
	}
	if writeTxn.IsCommitted() {
		return vv.V, nil
	}
	return "", consts.ErrTxnConflict
}

func (txn *Txn) Set(ctx context.Context, key, val string, writeIntent bool) error {
	return txn.kv.Set(ctx, key, val, txn.ID, writeIntent)
}
