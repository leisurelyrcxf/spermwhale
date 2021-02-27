package txn

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	utils2 "github.com/leisurelyrcxf/spermwhale/integration_test/utils"

	"github.com/leisurelyrcxf/spermwhale/proto/txnpb"

	"github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

type TransactionHolder interface {
	GetTxn(txnID types.TxnId) (*Txn, error)
	RemoveTxn(txn *Txn)
}

const (
	defaultReadTimeout  = time.Second * 10
	defaultClearTimeout = time.Second * 15
)

type Job func(ctx context.Context) error

func TransactionKey(id types.TxnId) string {
	return fmt.Sprintf("txn_%d", id)
}

func DecodeTxn(data []byte) (*Txn, error) {
	txn := &Txn{}
	if err := json.Unmarshal(data, txn); err != nil {
		return nil, err
	}
	return txn, nil
}

type TransactionInfo struct {
	ID    types.TxnId
	State types.TxnState
}

func NewTransactionInfo(id types.TxnId, state types.TxnState) TransactionInfo {
	return TransactionInfo{
		ID:    id,
		State: state,
	}
}

func (i *TransactionInfo) GetId() types.TxnId {
	return i.ID
}

func (i *TransactionInfo) GetState() types.TxnState {
	return i.State
}

func NewTransactionInfoFromPB(x *txnpb.Txn) TransactionInfo {
	return TransactionInfo{
		ID:    types.TxnId(x.Id),
		State: types.TxnState(x.State),
	}
}

func (i TransactionInfo) ToPB() *txnpb.Txn {
	return &txnpb.Txn{
		Id:    i.ID.Version(),
		State: i.State.ToPB(),
	}
}

type Txn struct {
	TransactionInfo

	WrittenKeys []string

	writeTasks     []*types.ListTask          `json:"-"`
	txnRecordTask  *types.ListTask            `json:"-"`
	lastWriteTasks map[string]*types.ListTask `json:"-"`
	cfg            types.TxnConfig            `json:"-"`
	kv             types.KV                   `json:"-"`
	oracle         *physical.Oracle           `json:"-"`
	store          *TransactionStore          `json:"-"`
	s              *Scheduler                 `json:"-"`
	h              TransactionHolder          `json:"-"`

	sync.Mutex `json:"-"`
}

func NewTxn(
	id types.TxnId,
	kv types.KV, cfg types.TxnConfig,
	oracle *physical.Oracle,
	store *TransactionStore, holder TransactionHolder,
	s *Scheduler) *Txn {
	return &Txn{
		TransactionInfo: TransactionInfo{
			ID:    id,
			State: types.TxnStateUncommitted,
		},
		lastWriteTasks: make(map[string]*types.ListTask),
		cfg:            cfg,
		kv:             kv,
		oracle:         oracle,
		store:          store,
		s:              s,
		h:              holder,
	}
}

func (txn *Txn) ioTaskIDOfKey(key string) string {
	return fmt.Sprintf("%s-%s", txn.Key(), key)
}

func (txn *Txn) Get(ctx context.Context, key string) (_ types.Value, err error) {
	if key == "" {
		return types.EmptyValue, errors.ErrEmptyKey
	}

	txn.Lock()
	defer txn.Unlock()

	if txn.State != types.TxnStateUncommitted {
		return types.EmptyValue, errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s, but got %s", types.TxnStateUncommitted, txn.State)
	}

	defer func() {
		if err != nil && errors.IsMustRollbackGetSetErr(err) {
			txn.State = types.TxnStateRollbacking
			_ = txn.rollback(ctx, txn.ID, true, err.Error())
		}
	}()

	var (
		vv            types.Value
		lastWriteTask = txn.lastWriteTasks[key]
	)
	if lastWriteTask != nil {
		ctx, cancel := context.WithTimeout(ctx, defaultReadTimeout)
		writeResult, writeErr := lastWriteTask.WaitFinish(ctx)
		cancel()
		if writeErr != nil {
			errors.CASErrorCode(writeErr, consts.ErrCodeFailedToWaitTask, consts.ErrCodeGetFailedToWaitTask)
			return types.EmptyValue, errors.Annotatef(writeErr, "read aborted due to previous write task failed")
		}
		assert.Must(writeResult == types.DummyResult)
	}
	if vv, err = txn.kv.Get(ctx, key, types.NewReadOption(txn.ID.Version())); err != nil {
		return types.EmptyValue, err
	}
	if lastWriteTask != nil && vv.Version != txn.ID.Version() {
		return types.EmptyValue, errors.Annotatef(errors.ErrTransactionConflict, "reason: previous write intent disappeared probably rollbacked")
	}
	if vv.Version == txn.ID.Version() || !vv.Meta.WriteIntent {
		// committed value
		return vv, nil
	}
	assert.Must(vv.Version < txn.ID.Version())
	writeTxn, err := txn.store.inferTransactionRecord(ctx, types.TxnId(vv.Version), txn.ID, key)
	if err != nil {
		return types.EmptyValue, errors.Annotatef(errors.ErrTransactionConflict, "reason: %v", err)
	}
	assert.Must(writeTxn.ID.Version() == vv.Version)
	if committed, _ := writeTxn.CheckCommitState(ctx, txn.ID, key); committed {
		return vv, nil
	}
	return types.EmptyValue, errors.ErrTransactionConflict
}

func (txn *Txn) CheckCommitState(ctx context.Context, callerTxn types.TxnId, conflictedKey string) (committed bool, rollbacked bool) {
	txn.Lock()
	defer txn.Unlock()

	return txn.checkCommitState(ctx, callerTxn, conflictedKey)
}

func (txn *Txn) checkCommitStateWithRetry(callerTxn types.TxnId, maxRetry int, conflictedKey string) (committed bool, rollbacked bool) {
	for i := 0; i < maxRetry; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		committed, rollbacked := txn.checkCommitState(ctx, callerTxn, conflictedKey)
		cancel()
		if committed || rollbacked {
			return committed, rollbacked
		}
		time.Sleep(time.Second)
	}
	return false, false
}

func (txn *Txn) checkCommitState(ctx context.Context, callerTxn types.TxnId, conflictedKey string) (committed bool, rollbacked bool) {
	switch txn.State {
	case types.TxnStateStaging:
		if len(txn.WrittenKeys) == 0 {
			glog.Fatalf("[CheckCommitState] len(txn.WrittenKeys) == 0, txn: %v", txn)
		}

		var writtenKeys = txn.WrittenKeys
		if conflictedKey != "" {
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
				return false, false
			}
			writtenKeys = append(append(append(make([]string, 0, len(txn.WrittenKeys)), txn.WrittenKeys[:conflictedKeyIndex]...),
				txn.WrittenKeys[conflictedKeyIndex+1:]...), conflictedKey)
		}
		for _, key := range writtenKeys {
			vv, err := txn.kv.Get(ctx, key, types.NewReadOption(txn.ID.Version()).SetExactVersion())
			if err != nil && !errors.IsNotExistsErr(err) {
				glog.Errorf("[CheckCommitState] kv.Get returns unexpected error: %v", err)
				return false, false
			}
			if err == nil {
				assert.Must(vv.Version == txn.ID.Version())
				if !vv.WriteIntent {
					txn.onCommitted(callerTxn, fmt.Sprintf("found committed during CheckCommitState: key '%s' committed", key)) // help commit since original txn coordinator may have gone
					return true, false
				}
				continue
			}
			assert.Must(errors.IsNotExistsErr(err))
			if key == conflictedKey || errors.IsNeedsRollbackErr(err) {
				txn.State = types.TxnStateRollbacking
				_ = txn.rollback(ctx, callerTxn, true, "found non exist key during CheckCommitState") // help rollback since original txn coordinator may have gone
				return false, true
			}
			return false, false
		}
		txn.onCommitted(callerTxn, "found committed during CheckCommitState: all keys exist") // help commit since original txn coordinator may have gone
		return true, false
	case types.TxnStateCommitted:
		return true, false
	case types.TxnStateRollbacking:
		_ = txn.rollback(ctx, callerTxn, false, fmt.Sprintf("found transaction in state '%s'", types.TxnStateRollbacking)) // help rollback since original txn coordinator may have gone
		return false, true
	case types.TxnStateRollbacked:
		return false, true
	default:
		panic(fmt.Sprintf("impossible transaction state %s", txn.State))
	}
}

func (txn *Txn) Set(ctx context.Context, key string, val []byte) error {
	if key == "" {
		return errors.ErrEmptyKey
	}

	txn.Lock()
	defer txn.Unlock()

	if txn.State != types.TxnStateUncommitted {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s, but got %s", types.TxnStateUncommitted, txn.State)
	}

	writeVal := types.NewValue(val, txn.ID.Version())
	writeTask := types.NewListTaskNoResult(
		txn.ioTaskIDOfKey(key),
		fmt.Sprintf("set-key-%s", key),
		txn.cfg.StaleWriteThreshold, func(ctx context.Context, _ interface{}) error {
			return txn.kv.Set(ctx, key, writeVal, types.WriteOption{})
		})
	if err := txn.s.ScheduleIOJob(writeTask); err != nil {
		txn.State = types.TxnStateRollbacking
		_ = txn.rollback(ctx, txn.ID, true, fmt.Sprintf("io schedule failed: '%v'", err.Error()))
		return err
	}
	if _, ok := txn.lastWriteTasks[key]; !ok {
		txn.WrittenKeys = append(txn.WrittenKeys, key)
	}
	txn.writeTasks = append(txn.writeTasks, writeTask)
	txn.lastWriteTasks[key] = writeTask
	return nil
}

func (txn *Txn) Commit(ctx context.Context) error {
	txn.Lock()
	defer txn.Unlock()

	if txn.State != types.TxnStateUncommitted {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s, but got %s", types.TxnStateUncommitted, txn.State)
	}

	if len(txn.WrittenKeys) == 0 {
		return nil
	}

	txn.State = types.TxnStateStaging
	txnRecordTask := types.NewListTaskNoResult(
		txn.Key(),
		fmt.Sprintf("write-txn-record-%s", txn.Key()),
		txn.cfg.StaleWriteThreshold, func(ctx context.Context, _ interface{}) error {
			return txn.writeTxnRecord(ctx)
		})
	if err := txn.s.ScheduleIOJob(txnRecordTask); err != nil {
		txn.State = types.TxnStateRollbacking
		_ = txn.rollback(ctx, txn.ID, true, fmt.Sprintf("io schedule failed: '%v'", err.Error()))
		return err
	}
	txn.txnRecordTask = txnRecordTask
	maxRetry := utils2.MaxInt(int(int64(txn.cfg.StaleWriteThreshold)/int64(time.Second))*3, 10)
	if _, recordErr := txn.txnRecordTask.WaitFinish(ctx); recordErr != nil {
		errors.CASErrorCode(recordErr, consts.ErrCodeFailedToWaitTask, consts.ErrCodeCommitFailedToWaitTask)
		if errors.IsMustRollbackCommitErr(recordErr) {
			// write record must failed
			txn.State = types.TxnStateRollbacking
			_ = txn.rollback(ctx, txn.ID, true, fmt.Sprintf("commit() returns rollbackable error: '%v'", recordErr))
			return recordErr
		}

		txn.cancelIOTasksAndWait()
		ctx = context.Background()
		record, recordExists, err := txn.store.loadTransactionRecordWithRetry(ctx, txn.ID, types.NewReadOption(math.MaxUint64), maxRetry)
		if err != nil {
			glog.Errorf("[Commit] can't get transaction record info in %s, err: %v", txn.cfg.StaleWriteThreshold*10, err)
			return recordErr
		}
		if !recordExists {
			// Transaction record not exists
			// There will be 3 cases:
			// 1. transaction has been rollbacked, conflictedKey must be gone
			// 2. transaction has been committed and cleared, conflictedKey must have been cleared (no write intent)
			// 3. transaction neither committed nor rollbacked
			val, keyExists, err := getKeyExactVersionWithRetry(ctx, txn.kv, txn.WrittenKeys[0], txn.ID.Version(), maxRetry)
			if err != nil {
				glog.Errorf("[Commit] can't get key info in %s, err: %v", txn.cfg.StaleWriteThreshold*10, err)
				return recordErr
			}
			if keyExists && !val.WriteIntent {
				// case 2
				assert.Must(val.Version == txn.ID.Version())
				txn.State = types.TxnStateCommitted
				return nil
			}
			txn.State = types.TxnStateRollbacking
			_ = txn.rollback(ctx, txn.ID, true, fmt.Sprintf("found transcation record not exists after a successful get with update timestamp cache option"))
			return recordErr
		}
		assert.Must(len(record.WrittenKeys) == len(txn.WrittenKeys))
		assert.Must(txn.ID == record.ID)
		txn.State = record.State
		// TODO add succeeded key as conflicted key
		committed, rollbacked := txn.checkCommitStateWithRetry(txn.ID, maxRetry, "")
		if committed {
			return nil
		}
		if rollbacked {
			assert.Must(txn.State == types.TxnStateRollbacked || txn.State == types.TxnStateRollbacking)
		}
		return recordErr
	}

	for _, writeTask := range txn.writeTasks {
		if _, keyErr := writeTask.WaitFinish(ctx); keyErr != nil {
			errors.CASErrorCode(keyErr, consts.ErrCodeFailedToWaitTask, consts.ErrCodeCommitFailedToWaitTask)
			if errors.IsMustRollbackCommitErr(keyErr) {
				// write record must failed
				txn.State = types.TxnStateRollbacking
				_ = txn.rollback(ctx, txn.ID, true, fmt.Sprintf("write key %s returns rollbackable error: '%v'", writeTask.ID, keyErr))
				return keyErr
			}

			txn.cancelIOTasksAndWait()
			committed, rollbacked := txn.checkCommitStateWithRetry(txn.ID, maxRetry, "")
			if committed {
				return nil
			}
			if rollbacked {
				assert.Must(txn.State == types.TxnStateRollbacked || txn.State == types.TxnStateRollbacking)
			}
			return keyErr
		}
	}

	txn.onCommitted(txn.ID, "commit by user")
	txn.gcIOTasks()
	return nil
}

func (txn *Txn) cancelIOTasksAndWait() {
	for _, writeTask := range txn.writeTasks {
		writeTask.Cancel()
	}
	if txn.txnRecordTask != nil {
		txn.txnRecordTask.Cancel()
	}
	for _, writeTask := range txn.writeTasks {
		_, _ = writeTask.WaitFinish(context.Background())
	}
	if txn.txnRecordTask != nil {
		_, _ = txn.txnRecordTask.WaitFinish(context.Background())
	}
	txn.gcIOTasks()
}

func (txn *Txn) gcIOTasks() {
	tasks := append(make([]*types.ListTask, 0, len(txn.writeTasks)), txn.writeTasks...)
	if txn.txnRecordTask != nil {
		tasks = append(tasks, txn.txnRecordTask)
	}
	txn.s.GCIOJobs(tasks)
}

func (txn *Txn) Rollback(ctx context.Context) error {
	txn.Lock()
	defer txn.Unlock()

	return txn.rollback(ctx, txn.ID, true, "rollback by user")
}

func (txn *Txn) rollback(ctx context.Context, callerTxn types.TxnId, createTxnRecordOnFailure bool, reason string) (err error) {
	if callerTxn != txn.ID && !txn.isTooStale() {
		return nil
	}
	txn.cancelIOTasksAndWait()
	if txn.State == types.TxnStateRollbacked {
		return nil
	}

	var verbose glog.Level = 10
	if callerTxn != txn.ID {
		verbose = 5
	}
	glog.V(verbose).Infof("rollbacking txn %d..., callerTxn: %d, reason: '%v'", txn.ID, callerTxn, reason)

	if txn.State != types.TxnStateUncommitted && txn.State != types.TxnStateRollbacking {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s or %s, but got %s", types.TxnStateUncommitted, types.TxnStateRollbacking, txn.State)
	}

	//txn.h.(*TransactionManager).rollbackedTxns.Set(txn.ID, txn)
	txn.State = types.TxnStateRollbacking
	for _, key := range txn.WrittenKeys {
		if removeErr := txn.kv.Set(ctx, key,
			types.NewValue(nil, txn.ID.Version()).SetNoWriteIntent(),
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
	txn.State = types.TxnStateRollbacked
	if callerTxn == txn.ID {
		txn.h.RemoveTxn(txn)
	}
	return nil
}

func (txn *Txn) Encode() []byte {
	return utils.JsonEncode(txn)
}

func (txn *Txn) Key() string {
	return TransactionKey(txn.ID)
}

func (txn *Txn) onCommitted(callerTxn types.TxnId, reason string) {
	txn.State = types.TxnStateCommitted
	if callerTxn != txn.ID && !txn.isTooStale() {
		return
	}

	if callerTxn != txn.ID {
		glog.V(11).Infof("clearing committed status for stale txn %d..., callerTxn: %d, reason: '%v'", txn.ID, callerTxn, reason)
	}

	_ = txn.s.ScheduleClearJob(
		types.NewTaskNoResult(
			txn.Key(),
			fmt.Sprintf("clear-%s-write-intents", txn.Key()),
			defaultClearTimeout, func(ctx context.Context) error {
				return txn.clearCommitted(ctx, callerTxn)
			}))
}

func (txn *Txn) clearCommitted(ctx context.Context, callerTxn types.TxnId) (err error) {
	txn.Lock()
	defer txn.Unlock()

	if txn.State != types.TxnStateCommitted {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %v, but got %v", types.TxnStateCommitted, txn.State)
	}

	for _, key := range txn.WrittenKeys {
		if setErr := txn.kv.Set(ctx, key,
			types.NewValue(nil, txn.ID.Version()).SetNoWriteIntent(),
			types.NewWriteOption().SetClearWriteIntent()); setErr != nil {
			if errors.IsNotExistsErr(setErr) {
				glog.Fatalf("Status corrupted: clear transaction key '%s' write intent failed: '%v'", key, setErr)
			} else {
				glog.Warningf("clear transaction key '%s' write intent failed: '%v'", key, setErr)
			}
			err = errors.Wrap(err, setErr)
		}
	}
	if err != nil {
		return err
	}
	if err := txn.removeTxnRecord(ctx); err != nil {
		return err
	}
	if callerTxn == txn.ID {
		txn.h.RemoveTxn(txn)
	}
	return nil
}

func (txn *Txn) writeTxnRecord(ctx context.Context) error {
	// set write intent so that other transactions can stop this txn from committing,
	// thus implement the safe-rollback functionality
	err := txn.kv.Set(ctx, txn.Key(), types.NewValue(txn.Encode(), txn.ID.Version()), types.WriteOption{})
	if err != nil {
		glog.Errorf("[writeTxnRecord] write transaction record failed: %v", err)
	}
	return err
}

func (txn *Txn) removeTxnRecord(ctx context.Context) error {
	err := txn.kv.Set(ctx, txn.Key(),
		types.NewValue(nil, txn.ID.Version()).SetNoWriteIntent(),
		types.NewWriteOption().SetRemoveVersion())
	if err != nil && !errors.IsNotExistsErr(err) {
		glog.Warningf("clear transaction record failed: %v", err)
		return err
	}
	return nil
}

func (txn *Txn) isTooStale() bool {
	return txn.oracle.IsTooStale(txn.ID.Version(), txn.cfg.StaleWriteThreshold)
}
