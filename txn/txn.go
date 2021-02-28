package txn

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	utils2 "github.com/leisurelyrcxf/spermwhale/integration_test/utils"

	"github.com/leisurelyrcxf/spermwhale/proto/txnpb"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
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

func InvalidTransactionInfo(id types.TxnId) TransactionInfo {
	return TransactionInfo{
		ID:    id,
		State: types.TxnStateInvalid,
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

	writeKeyTasks      []*types.ListTask          `json:"-"`
	lastWriteKeyTasks  map[string]*types.ListTask `json:"-"`
	txnRecordTask      *types.ListTask            `json:"-"`
	allIOTasksFinished bool                       `json:"-"`
	cfg                types.TxnConfig            `json:"-"`
	kv                 types.KV                   `json:"-"`
	store              *TransactionStore          `json:"-"`
	s                  *Scheduler                 `json:"-"`
	h                  TransactionHolder          `json:"-"`

	sync.Mutex `json:"-"`
}

func NewTxn(
	id types.TxnId,
	kv types.KV, cfg types.TxnConfig,
	store *TransactionStore, holder TransactionHolder,
	s *Scheduler) *Txn {
	return &Txn{
		TransactionInfo: TransactionInfo{
			ID:    id,
			State: types.TxnStateUncommitted,
		},
		lastWriteKeyTasks: make(map[string]*types.ListTask),
		cfg:               cfg,
		kv:                kv,
		store:             store,
		s:                 s,
		h:                 holder,
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
		if err != nil && errors.IsMustRollbackGetErr(err) {
			txn.State = types.TxnStateRollbacking
			_ = txn.rollback(ctx, txn.ID, true, err.Error())
		}
	}()

	var (
		vv            types.Value
		lastWriteTask = txn.lastWriteKeyTasks[key]
	)
	if lastWriteTask != nil {
		ctx, cancel := context.WithTimeout(ctx, defaultReadTimeout)
		if !lastWriteTask.WaitFinishWithContext(ctx) {
			cancel()
			return types.EmptyValue, errors.Annotatef(errors.ErrReadFailedToWaitWriteTask, "read timeout: %s", defaultReadTimeout)
		}
		cancel()
		if lastWriteTask.Err() != nil {
			return types.EmptyValue, errors.Annotatef(lastWriteTask.Err(), "read aborted due to previous write task failed")
		}
	}
	if vv, err = txn.kv.Get(ctx, key, types.NewReadOption(txn.ID.Version())); err != nil {
		return types.EmptyValue, err
	}
	if lastWriteTask != nil && vv.Version != txn.ID.Version() {
		return types.EmptyValue, errors.Annotatef(errors.ErrTransactionConflict, "reason: previous write intent disappeared probably rollbacked")
	}
	if vv.Version == txn.ID.Version() /* read after write */ ||
		!vv.Meta.WriteIntent /* committed value */ {
		return vv, nil
	}
	assert.Must(vv.Version < txn.ID.Version())
	writeTxn, err := txn.store.inferTransactionRecord(ctx, types.TxnId(vv.Version), txn.ID, key)
	if err != nil {
		return types.EmptyValue, errors.Annotatef(errors.ErrTransactionConflict, "reason: %v", err)
	}
	assert.Must(writeTxn.ID.Version() == vv.Version)
	if committed, _ := writeTxn.CheckCommitState(ctx, txn.ID, map[string]struct{}{key: {}}); committed {
		return vv, nil
	}
	return types.EmptyValue, errors.ErrTransactionConflict
}

func (txn *Txn) CheckCommitState(ctx context.Context, callerTxn types.TxnId, keysWithWriteIntent map[string]struct{}) (committed bool, rollbacked bool) {
	txn.Lock()
	defer txn.Unlock()

	return txn.checkCommitState(ctx, callerTxn, keysWithWriteIntent, 1)
}

func (txn *Txn) checkCommitState(ctx context.Context, callerTxn types.TxnId, keysWithWriteIntent map[string]struct{}, maxRetry int) (committed bool, rollbacked bool) {
	switch txn.State {
	case types.TxnStateStaging:
		if len(txn.WrittenKeys) == 0 {
			glog.Fatalf("[checkCommitState] len(txn.WrittenKeys) == 0, txn: %v", txn)
		}
		if len(keysWithWriteIntent) == len(txn.WrittenKeys) {
			assert.MustAllContain(keysWithWriteIntent, txn.WrittenKeys)
			txn.State = types.TxnStateCommitted
			txn.onCommitted(callerTxn, "found committed during CheckCommitState: len(keysWithWriteIntent) == len(txn.WrittenKeys)") // help commit since original txn coordinator may have gone
			return true, false
		}

		var inKeys, notInKeys []string
		for _, writtenKey := range txn.WrittenKeys {
			if _, ok := keysWithWriteIntent[writtenKey]; ok {
				inKeys = append(inKeys, writtenKey)
			} else {
				notInKeys = append(notInKeys, writtenKey)
			}
		}
		if len(inKeys) != len(keysWithWriteIntent) {
			glog.Fatalf("len(inKeys)(%v) != len(keysWithWriteIntent)(%v)", inKeys, keysWithWriteIntent)
			return false, false
		}
		notInKeysLen := len(notInKeys)
		for idx, key := range append(notInKeys, inKeys...) {
			vv, exists, err := getValueWrittenByTxn(ctx, txn.kv, key, txn.ID, maxRetry, callerTxn == txn.ID)
			if err != nil {
				return false, false
			}
			if exists {
				assert.Must(vv.Version == txn.ID.Version())
				if !vv.WriteIntent {
					txn.State = types.TxnStateCommitted
					txn.onCommitted(callerTxn, fmt.Sprintf("found committed during CheckCommitState: key '%s' committed", key)) // help commit since original txn coordinator may have gone
					return true, false
				}
				continue
			}
			if idx >= notInKeysLen {
				txn.State = types.TxnStateRollbacking
				_ = txn.rollback(ctx, callerTxn, true, "previous write intent disappeared") // help rollback since original txn coordinator may have gone
				return false, true
			}
			if vv.MaxReadVersion > txn.ID.Version() {
				txn.State = types.TxnStateRollbacking
				_ = txn.rollback(ctx, callerTxn, true, "found non exist key during CheckCommitState and is impossible to succeed in the future") // help rollback since original txn coordinator may have gone
				return false, true
			}
			return false, false
		}
		txn.State = types.TxnStateCommitted
		txn.onCommitted(callerTxn, "found committed during CheckCommitState: all keys exist") // help commit since original txn coordinator may have gone
		return true, false
	case types.TxnStateCommitted:
		return true, false
	case types.TxnStateRollbacking:
		_ = txn.rollback(ctx, callerTxn, false, fmt.Sprintf("transaction in state '%s'", types.TxnStateRollbacking)) // help rollback since original txn coordinator may have gone
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
	if _, ok := txn.lastWriteKeyTasks[key]; !ok {
		txn.WrittenKeys = append(txn.WrittenKeys, key)
	}
	txn.writeKeyTasks = append(txn.writeKeyTasks, writeTask)
	txn.lastWriteKeyTasks[key] = writeTask
	return nil
}

func (txn *Txn) Commit(ctx context.Context) error {
	txn.Lock()
	defer txn.Unlock()

	if txn.State != types.TxnStateUncommitted {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s, but got %s", types.TxnStateUncommitted, txn.State)
	}

	if len(txn.WrittenKeys) == 0 {
		txn.State = types.TxnStateCommitted
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
		_ = txn.rollback(ctx, txn.ID, true, fmt.Sprintf("write record io task schedule failed: '%v'", err.Error()))
		return err
	}

	txn.txnRecordTask = txnRecordTask
	txn.waitIOTasks(ctx, false)
	var lastNonRollbackableIOErr error
	for _, ioTask := range txn.getIOTasks() {
		if ioTaskErr := ioTask.Err(); ioTaskErr != nil {
			if errors.IsMustRollbackCommitErr(ioTaskErr) {
				// write record must failed
				txn.State = types.TxnStateRollbacking
				_ = txn.rollback(ctx, txn.ID, true, fmt.Sprintf("write key %s returns rollbackable error: '%v'", ioTask.ID, ioTaskErr))
				return ioTaskErr
			}
			lastNonRollbackableIOErr = ioTaskErr
		}
	}

	if lastNonRollbackableIOErr == nil {
		// succeeded
		txn.State = types.TxnStateCommitted
		txn.onCommitted(txn.ID, "all keys and txn record written successfully")
		return nil
	}

	// handle other kinds of error
	ctx = context.Background()
	maxRetry := utils2.MaxInt(int(int64(txn.cfg.StaleWriteThreshold)/int64(time.Second))*3, 10)
	if recordErr := txn.txnRecordTask.Err(); recordErr != nil {
		record, recordExists, err := txn.store.loadTransactionRecordWithRetry(ctx, txn.ID, true, maxRetry)
		if err != nil {
			txn.State = types.TxnStateInvalid
			glog.Errorf("[Commit] can't get transaction record info in %s, err: %v", txn.cfg.StaleWriteThreshold*10, err)
			return recordErr
		}
		if !recordExists {
			// Transaction record not exists
			// There will be 3 cases:
			// 1. transaction has been rollbacked, conflictedKey must be gone
			// 2. transaction has been committed and cleared, conflictedKey must have been cleared (no write intent)
			// 3. transaction neither committed nor rollbacked
			_, val, keyExists, err := txn.getAnyValueWrittenWithRetry(ctx, maxRetry)
			if err != nil {
				txn.State = types.TxnStateInvalid
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
			_ = txn.rollback(ctx, txn.ID, true, fmt.Sprintf("transcation record not exists after a successful get with update timestamp cache option"))
			return recordErr
		}
		assert.Must(txn.ID == record.ID)
		assert.MustEqualStrings(record.WrittenKeys, txn.WrittenKeys)
		txn.State = record.State
	}

	succeededKeys := make(map[string]struct{})
	for key, lastWriteKeyTask := range txn.lastWriteKeyTasks {
		if lastWriteKeyTask.Err() == nil {
			succeededKeys[key] = struct{}{}
		}
	}
	if len(succeededKeys) == len(txn.WrittenKeys) {
		assert.Must(txn.txnRecordTask.Err() != nil)
		txn.State = types.TxnStateCommitted
		txn.onCommitted(txn.ID, "after checking found all keys and txn record written successfully")
		return nil
	}
	committed, rollbacked := txn.checkCommitState(ctx, txn.ID, succeededKeys, maxRetry)
	if committed {
		assert.Must(txn.State == types.TxnStateCommitted)
		return nil
	}
	if rollbacked {
		assert.Must(txn.State.IsAborted())
		return lastNonRollbackableIOErr
	}
	txn.State = types.TxnStateInvalid
	return lastNonRollbackableIOErr
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
	txn.waitIOTasks(ctx, true)
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
			types.NewValue(nil, txn.ID.Version()).WithNoWriteIntent(),
			types.NewWriteOption().WithRemoveVersion()); removeErr != nil && !errors.IsNotExistsErr(removeErr) {
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

func (txn *Txn) getAnyValueWrittenWithRetry(ctx context.Context, maxRetry int) (key string, val types.Value, exists bool, err error) {
	for i := 0; i < maxRetry; {
		for _, key := range txn.WrittenKeys {
			ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			val, err = txn.kv.Get(ctx, key, types.NewReadOption(txn.ID.Version()).WithExactVersion().WithNotUpdateTimestampCache())
			cancel()
			if err == nil || errors.IsNotExistsErr(err) {
				return key, val, err == nil, nil
			}
			glog.Warningf("[getAnyValueWrittenByTxn] kv.Get conflicted key %s returns unexpected error: %v", key, err)
			time.Sleep(time.Second)
			i++
		}
	}
	assert.Must(err != nil)
	glog.Errorf("[getAnyValueWrittenByTxn] txn.kv.Get returns unexpected error: %v", err)
	return "", types.Value{}, false, err
}

func (txn *Txn) Encode() []byte {
	return utils.JsonEncode(txn)
}

func (txn *Txn) Key() string {
	return TransactionKey(txn.ID)
}

func (txn *Txn) waitIOTasks(ctx context.Context, forceCancel bool) {
	if txn.allIOTasksFinished {
		return
	}

	var (
		ioTasks        = txn.getIOTasks()
		cancelledTasks = make([]*types.ListTask, 0, len(ioTasks))
	)
	for _, ioTask := range ioTasks {
		if ctx.Err() == nil && !forceCancel {
			if !ioTask.WaitFinishWithContext(ctx) {
				ioTask.Cancel()
				cancelledTasks = append(cancelledTasks, ioTask)
			}
		} else {
			ioTask.Cancel()
			cancelledTasks = append(cancelledTasks, ioTask)
		}
	}
	for _, cancelledTask := range cancelledTasks {
		cancelledTask.WaitFinish()
	}
	for _, ioTask := range ioTasks { // TODO remove this in product
		assert.Must(ioTask.Finished())
	}
	txn.s.GCIOJobs(ioTasks)
	txn.allIOTasksFinished = true
}

func (txn *Txn) getIOTasks() []*types.ListTask {
	ioTasks := append(make([]*types.ListTask, 0, len(txn.writeKeyTasks)), txn.writeKeyTasks...)
	if txn.txnRecordTask != nil {
		ioTasks = append(ioTasks, txn.txnRecordTask)
	}
	return ioTasks
}

func (txn *Txn) onCommitted(callerTxn types.TxnId, reason string) {
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
			types.NewValue(nil, txn.ID.Version()).WithNoWriteIntent(),
			types.NewWriteOption().WithClearWriteIntent()); setErr != nil {
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
		glog.V(4).Infof("[writeTxnRecord] write transaction record failed: %v", err)
	}
	return err
}

func (txn *Txn) removeTxnRecord(ctx context.Context) error {
	err := txn.kv.Set(ctx, txn.Key(),
		types.NewValue(nil, txn.ID.Version()).WithNoWriteIntent(),
		types.NewWriteOption().WithRemoveVersion())
	if err != nil && !errors.IsNotExistsErr(err) {
		glog.Warningf("clear transaction record failed: %v", err)
		return err
	}
	return nil
}

func (txn *Txn) isTooStale() bool {
	return utils.IsTooStale(txn.ID.Version(), txn.cfg.StaleWriteThreshold)
}
