package txn

import (
	"context"
	"fmt"
	"github.com/leisurelyrcxf/spermwhale/errors"

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
	ID          uint64
	WrittenKeys []string

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
	if len(txn.WrittenKeys) == 0 {
		glog.Fatalf("len(txn.WrittenKeys) == 0")
	}
	if txn.state == StateUnknown {
		_ = txn.fetchState()
	}
	return txn.state == StateCommitted
}

func (txn *Txn) Get(ctx context.Context, key string) (types.Value, error) {
	vv, err := txn.kv.Get(ctx, key, types.NewReadOption(txn.ID, false))
	if err != nil {
		return types.Value{}, err
	}
	assert.Must(vv.Version <= txn.ID)
	if !vv.Meta.WriteIntent {
		// committed value
		return vv, nil
	}
	writeTxn, err := txn.txnStore.GetTxn(ctx, vv.Version)
	if err != nil {
		return types.Value{}, errors.Annotatef(consts.ErrTxnConflict, "reason: %v", err)
	}
	if writeTxn.IsCommitted() {
		writeTxn.OnCommitted(ctx)
		return vv, nil
	}
	return types.Value{}, consts.ErrTxnConflict
}

func (txn *Txn) Set(ctx context.Context, key string, val types.Value) error {
	return
}

func (txn *Txn) OnCommitted(ctx context.Context) {
	for _, key := range txn.WrittenKeys {
		if err := txn.kv.Set(ctx, key, types.Value{
			Meta: types.Meta{
				Version: txn.ID,
			},
		}, types.WriteOption{
			ClearWriteIntent: true,
		}); err != nil {
			glog.Warningf("clear txn key %s failed: %v", key, err)
			return
		}
	}

	if err := txn.kv.Set(ctx, txn.key(), types.Value{
		Meta: types.Meta{
			Version: txn.ID,
		},
	}, types.WriteOption{
		RemoveVersion: true,
	}); err != nil {
		glog.Warningf("clear transaction record failed: %v", err)
	}
}

func (txn *Txn) Encode() string {
	return string(utils.JsonEncode(txn))
}

func (txn *Txn) key() string {
	return fmt.Sprintf("txn_%d", txn.ID)
}

func (txn *Txn) fetchState(ctx context.Context) {
	for _, key := range txn.WrittenKeys {
		vv, err := txn.kv.Get(ctx, key, types.NewReadOption(txn.ID, true))
		if err != nil && err. {
			return
		}
		if vv.
	}
}

func Decode(b string) (txn *Txn) {

}
