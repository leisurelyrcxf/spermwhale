package txn

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/leisurelyrcxf/spermwhale/utils"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"
	"github.com/leisurelyrcxf/spermwhale/types"
)

type State string

const (
	StateUnknown     = ""
	StateUncommitted = "uncommitted"
	StateStaging     = "staging"
	StateCommitted   = "committed"
	StateRollbacked  = "rollbacked"
)

type Job func(ctx context.Context) error

func TransactionKey(id uint64) string {
	return fmt.Sprintf("txn_%d", id)
}

func DecodeTxn(data []byte) (*Txn, error) {
	txn := &Txn{}
	if err := json.Unmarshal(data, txn); err != nil {
		return nil, err
	}
	return txn, nil
}

type Txn struct {
	ID          uint64
	WrittenKeys []string
	State       State

	kv             types.KV          `json:"-"`
	staleThreshold time.Duration     `json:"-"`
	oracle         *physical.Oracle  `json:"-"`
	store          *TransactionStore `json:"-"`
	asyncJobs      chan<- Job        `json:"-"`
	sync.Mutex     `json:"-"`
}

func NewTxn(
	id uint64,
	kv types.KV, staleThreshold time.Duration,
	oracle *physical.Oracle, store *TransactionStore,
	asyncJobs chan<- Job) *Txn {
	return &Txn{
		ID:             id,
		State:          StateUncommitted,
		kv:             kv,
		staleThreshold: staleThreshold,
		oracle:         oracle,
		store:          store,
		asyncJobs:      asyncJobs,
	}
}

func (txn *Txn) Get(ctx context.Context, key string) (types.Value, error) {
	txn.Lock()
	defer txn.Unlock()

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
	if writeTxn.checkCommitState(ctx) {
		writeTxn.onCommitted()
		return vv, nil
	}
	return types.EmptyValue, errors.ErrTxnConflict
}

func (txn *Txn) Set(ctx context.Context, key string, val []byte) error {
	return txn.kv.Set(ctx, key, types.NewValue(val, txn.ID), types.WriteOption{})
}

func (txn *Txn) Commit(ctx context.Context) error {
	// TODO change to async
	// set write intent so that other transactions can stop this txn from committing,
	// thus implement the safe-rollback functionality
	err := txn.kv.Set(ctx, txn.Key(), types.NewValue(txn.Encode(), txn.ID), types.WriteOption{})
	if err != nil {
		glog.Errorf("[Commit] write transaction record failed: %v, rollbacking...", err)
		_ = txn.Rollback(ctx)
		return err
	}

	txn.onCommitted()
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

func (txn *Txn) Encode() []byte {
	return utils.JsonEncode(txn)
}

func (txn *Txn) Key() string {
	return TransactionKey(txn.ID)
}

func (txn *Txn) onCommitted() {
	txn.asyncJobs <- func(ctx context.Context) error {
		return txn.clearCommitted(ctx)
	}
}

func (txn *Txn) clearCommitted(ctx context.Context) (err error) {
	for _, key := range txn.WrittenKeys {
		if setErr := txn.kv.Set(ctx, key,
			types.NewValue(nil, txn.ID).SetNoWriteIntent(),
			types.NewWriteOption().SetClearWriteIntent()); setErr != nil {
			glog.Warningf("clear transaction key '%s' write intent failed: '%v'", key, setErr)
			err = errors.Wrap(err, setErr)
		}
	}
	err = errors.Wrap(err, txn.removeTxnRecord(ctx))
	return err
}

func (txn *Txn) removeTxnRecord(ctx context.Context) error {
	err := txn.kv.Set(ctx, txn.Key(),
		types.NewValue(nil, txn.ID).SetNoWriteIntent(),
		types.NewWriteOption().SetRemoveVersion())
	if err != nil {
		glog.Warningf("clear transaction record failed: %v", err)
	}
	return err
}

func (txn *Txn) checkCommitState(ctx context.Context) (committed bool) {
	switch txn.State {
	case StateCommitted:
		return true
	case StateStaging:
		if len(txn.WrittenKeys) == 0 {
			glog.Fatalf("[checkCommitState] len(txn.WrittenKeys) == 0, txn: %v", txn)
		}

		for _, key := range txn.WrittenKeys {
			vv, err := txn.kv.Get(ctx, key, types.NewReadOption(txn.ID).SetExactVersion())
			if err != nil && !errors.IsNotExistsErr(err) {
				glog.Errorf("[checkCommitState] kv.Get returns unexpected error: %v", err)
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
