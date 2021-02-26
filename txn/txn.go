package txn

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

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
	StateRollbacking = "rollbacking"
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

	writtenKeyMap map[string]struct{}
	cfg           types.TxnConfig   `json:"-"`
	kv            types.KV          `json:"-"`
	oracle        *physical.Oracle  `json:"-"`
	store         *TransactionStore `json:"-"`
	asyncJobs     chan<- Job        `json:"-"`

	sync.Mutex `json:"-"`
}

func NewTxn(
	id uint64,
	kv types.KV, cfg types.TxnConfig,
	oracle *physical.Oracle, store *TransactionStore,
	asyncJobs chan<- Job) *Txn {
	return &Txn{
		ID:    id,
		State: StateUncommitted,

		writtenKeyMap: make(map[string]struct{}),
		cfg:           cfg,
		kv:            kv,
		oracle:        oracle,
		store:         store,
		asyncJobs:     asyncJobs,
	}
}

func (txn *Txn) GetID() uint64 {
	return txn.ID
}

func (txn *Txn) Get(ctx context.Context, key string) (types.Value, error) {
	txn.Lock()
	defer txn.Unlock()

	if txn.State != StateUncommitted {
		return types.EmptyValue, errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %v, but got %v", StateUncommitted, txn.State)
	}

	vv, err := txn.kv.Get(ctx, key, types.NewReadOption(txn.ID))
	if err != nil {
		return types.EmptyValue, err
	}
	if vv.Version == txn.ID || !vv.Meta.WriteIntent {
		// committed value
		return vv, nil
	}
	assert.Must(vv.Version < txn.ID)
	writeTxn, err := txn.store.LoadTransactionRecord(ctx, vv.Version, key)
	if err != nil {
		return types.EmptyValue, errors.Annotatef(errors.ErrTransactionConflict, "reason: %v", err)
	}
	assert.Must(writeTxn.ID == vv.Version)
	if writeTxn.CheckCommitState(ctx, txn.ID, key) {
		return vv, nil
	}
	return types.EmptyValue, errors.ErrTransactionConflict
}

func (txn *Txn) CheckCommitState(ctx context.Context, callerTxn uint64, conflictedKey string) (committed bool) {
	txn.Lock()
	defer txn.Unlock()

	switch txn.State {
	case StateStaging:
		if len(txn.WrittenKeys) == 0 {
			glog.Fatalf("[CheckCommitState] len(txn.WrittenKeys) == 0, txn: %v", txn)
		}

		conflictedKeyIndex := func() int {
			for idx, key := range txn.WrittenKeys {
				if key == conflictedKey {
					return idx
				}
			}
			return -1
		}()
		if conflictedKeyIndex == -1 {
			glog.Fatalf("status corrupted, transaction record %v doesn't contain its key %s", txn, conflictedKey)
			return false
		}
		writtenKeys := append(append(append(
			make([]string, 0, len(txn.WrittenKeys)), txn.WrittenKeys[:conflictedKeyIndex]...),
			txn.WrittenKeys[conflictedKeyIndex+1:]...),
			conflictedKey)
		for _, key := range writtenKeys {
			vv, err := txn.kv.Get(ctx, key, types.NewReadOption(txn.ID).SetExactVersion())
			if err != nil && !errors.IsNotExistsErr(err) {
				glog.Errorf("[CheckCommitState] kv.Get returns unexpected error: %v", err)
				return false
			}
			if err == nil {
				assert.Must(vv.Version == txn.ID)
				if !vv.WriteIntent {
					txn.onCommitted(callerTxn) // help commit since original txn coordinator may have gone
					return true
				}
				continue
			}
			assert.Must(errors.IsNotExistsErr(err))
			if errors.IsNeedsRollbackErr(err) {
				txn.State = StateRollbacking
				_ = txn.rollback(ctx, callerTxn, true, "found non exist key during CheckCommitState") // help rollback since original txn coordinator may have gone
			}
			return false
		}
		txn.onCommitted(callerTxn) // help commit if original txn coordinator was gone
		return true
	case StateCommitted:
		return true
	case StateRollbacking:
		_ = txn.rollback(ctx, callerTxn, false, fmt.Sprintf("found transaction in state '%s'", StateRollbacking)) // help rollback since original txn coordinator may have gone
		return false
	case StateRollbacked:
		return false
	default:
		panic(fmt.Sprintf("impossible transaction state %s", txn.State))
	}
}

func (txn *Txn) Set(ctx context.Context, key string, val []byte) error {
	txn.Lock()
	defer txn.Unlock()

	if txn.State != StateUncommitted {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %v, but got %v", StateUncommitted, txn.State)
	}

	txn.addWrittenKey(key)
	err := txn.kv.Set(ctx, key, types.NewValue(val, txn.ID), types.WriteOption{})
	if err != nil {
		return err
	}
	return nil
}

func (txn *Txn) Commit(ctx context.Context) error {
	txn.Lock()
	defer txn.Unlock()

	if txn.State != StateUncommitted {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %v, but got %v", StateUncommitted, txn.State)
	}

	if len(txn.WrittenKeys) == 0 {
		return nil
	}

	txn.State = StateStaging
	// TODO change to async
	if err := txn.writeTxnRecord(ctx); err != nil {
		return err
	}

	txn.onCommitted(txn.ID)
	txn.State = StateCommitted
	return nil
}

func (txn *Txn) Rollback(ctx context.Context) error {
	txn.Lock()
	defer txn.Unlock()

	return txn.rollback(ctx, txn.ID, true, "rollback by user")
}

func (txn *Txn) rollback(ctx context.Context, callerTxn uint64, createTxnRecordOnFailure bool, reason string) (err error) {
	if callerTxn != txn.ID && !txn.isTooStale() {
		return nil
	}
	var verbose glog.Level = 10
	if callerTxn != txn.ID {
		verbose = 5
	}
	glog.V(verbose).Infof("rollbacking txn %d..., callerTxn: %d, reason: '%v'", txn.ID, callerTxn, reason)

	if txn.State == StateRollbacked {
		return nil
	}

	if txn.State != StateUncommitted && txn.State != StateRollbacking {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %v or %v, but got %v", StateUncommitted, StateRollbacking, txn.State)
	}

	// assert.Must(len(txn.WrittenKeys) > 0)

	txn.State = StateRollbacking
	for _, key := range txn.WrittenKeys {
		if removeErr := txn.kv.Set(ctx, key,
			types.NewValue(nil, txn.ID).SetNoWriteIntent(),
			types.NewWriteOption().SetRemoveVersion()); removeErr != nil && !errors.IsNotExistsErr(removeErr) {
			glog.Warningf("rollback key %v failed: '%v'", key, removeErr)
			err = errors.Wrap(err, removeErr)
		}
	}
	if err != nil {
		if createTxnRecordOnFailure {
			_ = txn.writeTxnRecord(ctx) // make life easier for other transactions
		}
		return err
	}
	if err := txn.removeTxnRecord(ctx); err != nil {
		return err
	}
	txn.State = StateRollbacked
	return nil
}

func (txn *Txn) Encode() []byte {
	return utils.JsonEncode(txn)
}

func (txn *Txn) Key() string {
	return TransactionKey(txn.ID)
}

func (txn *Txn) AddWrittenKey(key string) {
	txn.Lock()
	defer txn.Unlock()

	txn.addWrittenKey(key)
}

func (txn *Txn) addWrittenKey(key string) {
	if txn.hasWritten(key) {
		return
	}
	txn.WrittenKeys = append(txn.WrittenKeys, key)
}

func (txn *Txn) hasWritten(key string) bool {
	_, ok := txn.writtenKeyMap[key]
	return ok
}

func (txn *Txn) onCommitted(callerTxn uint64) {
	if callerTxn != txn.ID && !txn.isTooStale() {
		return
	}

	txn.asyncJobs <- func(ctx context.Context) error {
		return txn.clearCommitted(ctx)
	}
}

func (txn *Txn) clearCommitted(ctx context.Context) (err error) {
	txn.Lock()
	defer txn.Unlock()

	if txn.State != StateCommitted {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %v, but got %v", StateCommitted, txn.State)
	}

	for _, key := range txn.WrittenKeys {
		if setErr := txn.kv.Set(ctx, key,
			types.NewValue(nil, txn.ID).SetNoWriteIntent(),
			types.NewWriteOption().SetClearWriteIntent()); setErr != nil {
			glog.Warningf("clear transaction key '%s' write intent failed: '%v'", key, setErr)
			err = errors.Wrap(err, setErr)
		}
	}
	if err != nil {
		return err
	}
	return txn.removeTxnRecord(ctx)
}

func (txn *Txn) writeTxnRecord(ctx context.Context) error {
	// set write intent so that other transactions can stop this txn from committing,
	// thus implement the safe-rollback functionality
	err := txn.kv.Set(ctx, txn.Key(), types.NewValue(txn.Encode(), txn.ID), types.WriteOption{})
	if err != nil {
		glog.Errorf("[writeTxnRecord] write transaction record failed: %v, rollbacking...", err)
	}
	return err
}

func (txn *Txn) removeTxnRecord(ctx context.Context) error {
	err := txn.kv.Set(ctx, txn.Key(),
		types.NewValue(nil, txn.ID).SetNoWriteIntent(),
		types.NewWriteOption().SetRemoveVersion())
	if err != nil && !errors.IsNotExistsErr(err) {
		glog.Warningf("clear transaction record failed: %v", err)
		return err
	}
	return nil
}

func (txn *Txn) isTooStale() bool {
	return txn.oracle.IsTooStale(txn.ID, txn.cfg.StaleWriteThreshold)
}
