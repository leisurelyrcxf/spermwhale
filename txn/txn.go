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
	ID          uint64
	WrittenKeys []string
	State       State

	kv             types.KV          `json:"-"`
	staleThreshold time.Duration     `json:"-"`
	oracle         *physical.Oracle  `json:"-"`
	store          *TransactionStore `json:"-"`
}

func NewTxn(
	id uint64,
	kv types.KV, staleThreshold time.Duration,
	oracle *physical.Oracle, store *TransactionStore) *Txn {
	return &Txn{
		ID:             id,
		kv:             kv,
		staleThreshold: staleThreshold,
		oracle:         oracle,
		store:          store,
	}
}

func (txn *Txn) Get(ctx context.Context, key string) (types.Value, error) {
	vv, err := txn.kv.Get(ctx, key, types.NewReadOption(txn.ID))
	if err != nil {
		return types.EmptyValue, err
	}
	assert.Must(vv.Version <= txn.ID)
	if !vv.Meta.WriteIntent {
		// committed value
		return vv, nil
	}
	writeTxn, err := txn.store.GetTxn(ctx, vv.Version)
	if err != nil {
		return types.EmptyValue, errors.Annotatef(errors.ErrTxnConflict, "reason: %v", err)
	}
	if writeTxn.checkCommitted(ctx) {
		_ = writeTxn.onCommitted(ctx)
		return vv, nil
	}
	return types.EmptyValue, errors.ErrTxnConflict
}

func (txn *Txn) Set(ctx context.Context, key string, val []byte) error {
	return txn.kv.Set(ctx, key, types.NewValue(val, txn.ID), types.WriteOption{})
}

func (txn *Txn) Commit(ctx context.Context) error {
	// TODO change to async
	// set write intent to true so that other transactions can stop this txn from committing,
	// thus implement the safe-rollback functionality
	err := txn.kv.Set(ctx, txn.key(), types.NewValue(txn.Encode(), txn.ID), types.WriteOption{})
	if err != nil {
		return err
	}

	_ = txn.onCommitted(ctx)
	return nil
}

func (txn *Txn) Rollback(ctx context.Context) (err error) {
	for _, key := range txn.WrittenKeys {
		if oneErr := txn.kv.Set(ctx, key,
			types.NewValue(nil, txn.ID).SetNoWriteIntent(),
			types.NewWriteOption().SetRemoveVersion()); oneErr != nil {
			glog.Warningf("rollback key %v failed: '%v'", key, oneErr)
			err = errors.Wrap(err, oneErr)
		}
	}
	err = errors.Wrap(err, txn.removeTxnRecord(ctx))
	txn.State = StateRollbacked
	return err
}

func (txn *Txn) onCommitted(ctx context.Context) (err error) {
	for _, key := range txn.WrittenKeys {
		if setErr := txn.kv.Set(ctx, key,
			types.NewValue(nil, txn.ID).SetNoWriteIntent(),
			types.NewWriteOption().SetClearWriteIntent()); setErr != nil {
			glog.Warningf("clear transaction key '%s' failed: '%v'", key, setErr)
			err = errors.Wrap(err, setErr)
		}
	}
	err = errors.Wrap(err, txn.removeTxnRecord(ctx))
	return err
}

func (txn *Txn) checkCommitted(ctx context.Context) (committed bool) {
	switch txn.State {
	case StateCommitted:
		return true
	case StateUnknown, StateRollbacked:
		return false
	case StateStaging:
		if len(txn.WrittenKeys) == 0 {
			glog.Fatalf("[checkCommitted] len(txn.WrittenKeys) == 0, txn: %v", txn)
		}

		for _, key := range txn.WrittenKeys {
			vv, err := txn.kv.Get(ctx, key, types.NewReadOption(txn.ID).SetExactVersion())
			if err != nil && !errors.IsNotExistsErr(err) {
				glog.Errorf("[checkCommitted] kv.Get returns unexpected error: %v", err)
				return false
			}
			if err == nil {
				assert.Must(vv.Version == txn.ID)
				continue
			}
			assert.Must(errors.IsNotExistsErr(err))
			if errors.IsNeedsRollbackErr(err) {
				_ = txn.Rollback(ctx)
			}
			return false
		}
		return true
	default:
		return false
	}
}

func (txn *Txn) removeTxnRecord(ctx context.Context) error {
	err := txn.kv.Set(ctx, txn.key(),
		types.NewValue(nil, txn.ID).SetNoWriteIntent(),
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
	return TransactionKey(txn.ID)
}

func DecodeTxn(data []byte) (*Txn, error) {
	txn := &Txn{}
	if err := json.Unmarshal(data, txn); err != nil {
		return nil, err
	}
	return txn, nil
}
