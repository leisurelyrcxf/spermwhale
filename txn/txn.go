package txn

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

type State string

const (
	StateUnknown    = ""
	StateStaging    = "staging"
	StateCommitted  = "committed"
	StateRollbacked = "rollbacked"
)

func TransactionKey(id uint64) string {
	return fmt.Sprintf("txn_%d", id)
}

type Txn struct {
	id          uint64
	WrittenKeys []string
	state       State

	staleThreshold time.Duration    `json:"-"`
	oracle         *physical.Oracle `json:"-"`
	kv             types.KV         `json:"-"`
	store          Store            `json:"-"`
}

func NewTxn(id uint64, kv types.KV) *Txn {
	return &Txn{
		id: id,
		kv: kv,
	}
}

func (txn *Txn) CheckCommitted(ctx context.Context) (committed bool) {
	if txn.state == StateCommitted {
		return true
	}
	if txn.state == StateRollbacked {
		return false
	}
	for _, key := range txn.WrittenKeys {
		vv, err := txn.kv.Get(ctx, key, types.NewReadOption(txn.id).SetExactVersion())
		if err != nil && !errors.IsKeyOrVersionNotExistsErr(err) {
			return StateUnknown
		}
		if err == nil {
			assert.Must(vv.Version == txn.id)
			continue
		}
	}
	return txn.state == StateCommitted
}

func (txn *Txn) IsTooStale(ctx context.Context) bool {
	now := txn.oracle.MustFetchTimestamp()
}

func (txn *Txn) Get(ctx context.Context, key string) (types.Value, error) {
	vv, err := txn.kv.Get(ctx, key, types.NewReadOption(txn.id))
	if err != nil {
		return types.EmptyValue, err
	}
	assert.Must(vv.Version <= txn.id)
	if !vv.Meta.WriteIntent {
		// committed value
		return vv, nil
	}
	writeTxn, err := txn.store.GetTxn(ctx, vv.Version)
	if err != nil {
		return types.EmptyValue, errors.Annotatef(errors.ErrTxnConflict, "reason: %v", err)
	}
	if writeTxn.CheckCommitted(ctx) {
		writeTxn.OnCommitted(ctx)
		return vv, nil
	}
	return types.EmptyValue, errors.ErrTxnConflict
}

func (txn *Txn) Set(ctx context.Context, key string, val []byte) error {
	return txn.kv.Set(ctx, key, types.NewValue(val, txn.id), types.WriteOption{})
}

func (txn *Txn) Commit(ctx context.Context) error {
	// TODO change to async
	// set write intent to true so that other transactions can stop this txn from committing,
	// thus implement the safe-rollback functionality
	err := txn.kv.Set(ctx, txn.key(), types.NewValue(txn.Encode(), txn.id), types.WriteOption{})
	if err != nil {
		return err
	}
	return nil
}

func (txn *Txn) Rollback(ctx context.Context) (err error) {
	for _, key := range txn.WrittenKeys {
		if oneErr := txn.kv.Set(ctx, key,
			types.NewValue(nil, txn.id).SetNoWriteIntent(),
			types.NewWriteOption().SetRemoveVersion()); oneErr != nil {
			glog.Warningf("rollback key %v failed: '%v'", key, oneErr)
			err = errors.Wrap(err, oneErr)
		}
	}
	err = errors.Wrap(err, txn.removeTxnRecord(ctx))
	txn.state = StateRollbacked
	return err
}

func (txn *Txn) OnCommitted(ctx context.Context) {
	for _, key := range txn.WrittenKeys {
		if err := txn.kv.Set(ctx, key,
			types.NewValue(nil, txn.id).SetNoWriteIntent(),
			types.NewWriteOption().SetClearWriteIntent()); err != nil {
			glog.Warningf("clear transaction key '%s' failed: '%v'", key, err)
			return
		}
	}
	_ = txn.removeTxnRecord(ctx)
}

func (txn *Txn) removeTxnRecord(ctx context.Context) error {
	err := txn.kv.Set(ctx, txn.key(),
		types.NewValue(nil, txn.id).SetNoWriteIntent(),
		types.NewWriteOption().SetRemoveVersion())
	if err != nil {
		glog.Warningf("clear transaction record failed: %v", err)
	}
	return err
}

func (txn *Txn) Encode() []byte {
	return utils.JsonEncode(txn)
}

func (txn *Txn) key() string {
	return TransactionKey(txn.id)
}

func DecodeTxn(data []byte) (*Txn, error) {
	txn := &Txn{}
	if err := json.Unmarshal(data, txn); err != nil {
		return nil, err
	}
	return txn, nil
}
