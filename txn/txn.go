package txn

import (
	"context"

	"google.golang.org/appengine/log"

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

	txnStore Store
	state    State
}

func NewTxn(id uint64, kv types.KV) *Txn {
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
	if !vv.Meta.WriteIntent {
		// committed value
		return vv.V, nil
	}
	writeTxn, err := txn.txnStore.GetTxn(ctx, vv.Version)
	if err != nil {
		return "", err
	}
	if writeTxn.IsCommitted() {
		if err := txn.kv.Set(ctx, key, ""); err != nil {
			log.Warningf("clear write intent failed: '%v'", err)
		}
		return vv.V, nil
	}
	return "", consts.ErrTxnConflict
}

func (txn *Txn) Set(ctx context.Context, key, val string, writeIntent bool) error {
	return txn.kv.Set(ctx, key, val, txn.ID, writeIntent)
}
