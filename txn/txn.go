package txn

import (
	"context"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/utils"

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
	ID   uint64
	Keys []string

	kv       types.KV `json:"-"`
	txnStore Store    `json:"-"`
	state    State    `json:"-"`
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

		return vv.V, nil
	}
	return "", consts.ErrTxnConflict
}

func (txn *Txn) Set(ctx context.Context, key, val string, writeIntent bool) error {
	return txn.kv.Set(ctx, key, val, txn.ID, writeIntent)
}

func (txn *Txn) OnCommitted(ctx context.Context) {
	if err := txn.kv.Set(ctx, key, "", types.WriteOption{
		Meta:             types.Meta{Version: writeTxn.ID},
		ClearWriteIntent: true,
	}); err != nil {
		glog.Warningf("clear write intent of txn %d failed: '%v'", writeTxn.ID, err)
	}
}

func (txn *Txn) Encode() string {
	return string(utils.JsonEncode(txn))
}

func Decode(b string) (txn *Txn) {

}
