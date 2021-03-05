package txn

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/proto/txnpb"
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

	writeKeyTasks      []*types.ListTask
	lastWriteKeyTasks  map[string]*types.ListTask
	txnRecordTask      *types.ListTask
	preventWriteFlags  map[string]bool
	allIOTasksFinished bool
	cfg                types.TxnConfig
	kv                 types.KVCC
	store              *TransactionStore
	s                  *Scheduler
	h                  TransactionHolder

	sync.Mutex `json:"-"`
}

func NewTxn(
	id types.TxnId,
	kv types.KVCC, cfg types.TxnConfig,
	store *TransactionStore, holder TransactionHolder,
	s *Scheduler) *Txn {
	return &Txn{
		TransactionInfo: TransactionInfo{
			ID:    id,
			State: types.TxnStateUncommitted,
		},
		lastWriteKeyTasks: make(map[string]*types.ListTask),
		preventWriteFlags: make(map[string]bool),
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

	defer func() {
		if err != nil && errors.IsMustRollbackGetErr(err) {
			txn.State = types.TxnStateRollbacking
			_ = txn.rollback(ctx, txn.ID, true, err.Error())
		}
	}()

	for i := 0; ; i++ {
		val, err := txn.get(ctx, key)
		if err == nil || !errors.IsRetryableGetErr(err) || i >= consts.MaxRetryTxnGet-1 || ctx.Err() != nil {
			return val, err
		}
		if errors.GetErrorCode(err) != consts.ErrCodeReadRollbackedData {
			rand.Seed(time.Now().UnixNano())
			time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
		}
	}
}

func (txn *Txn) get(ctx context.Context, key string) (_ types.Value, err error) {
	if txn.State != types.TxnStateUncommitted {
		return types.EmptyValue, errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s, but got %s", types.TxnStateUncommitted, txn.State)
	}

	var (
		vv            types.ValueCC
		lastWriteTask = txn.lastWriteKeyTasks[key]
	)
	if lastWriteTask != nil {
		ctx, cancel := context.WithTimeout(ctx, defaultReadTimeout)
		if !lastWriteTask.WaitFinishWithContext(ctx) {
			cancel()
			return types.EmptyValue, errors.Annotatef(errors.ErrReadAfterWriteFailed, "wait write task timeouted after %s", defaultReadTimeout)
		}
		cancel()
		if writeErr := lastWriteTask.Err(); writeErr != nil {
			return types.EmptyValue, errors.Annotatef(errors.ErrReadAfterWriteFailed, "previous error: '%v'", writeErr)
		}
	}
	if lastWriteTask == nil {
		vv, err = txn.kv.Get(ctx, key, types.NewKVCCReadOption(txn.ID.Version()))
	} else {
		vv, err = txn.kv.Get(ctx, key, types.NewKVCCReadOption(txn.ID.Version()).WithExactVersion(txn.ID.Version()))
	}
	if //noinspection ALL
	vv.MaxReadVersion > txn.ID.Version() {
		txn.preventWriteFlags[key] = true
	}
	if err != nil {
		if lastWriteTask != nil && errors.IsNotExistsErr(err) {
			return types.EmptyValue, errors.Annotatef(errors.ErrWriteReadConflict, "reason: previous write intent disappeared probably rollbacked")
		}
		return types.EmptyValue, err
	}
	assert.Must((lastWriteTask == nil && vv.Version < txn.ID.Version()) || (lastWriteTask != nil && vv.Version == txn.ID.Version()))
	if !vv.Meta.HasWriteIntent() {
		// committed value
		return vv.Value, nil
	}
	if vv.Version == txn.ID.Version() {
		// read after write
		return vv.Value, nil
	}
	assert.Must(vv.Version < txn.ID.Version())
	var (
		keyWithWriteIntent = map[string]struct{}{key: {}}
		preventFutureWrite = utils.IsTooStale(vv.Version, txn.cfg.StaleWriteThreshold)
	)
	writeTxn, err := txn.store.inferTransactionRecordWithRetry(
		ctx,
		types.TxnId(vv.Version), txn,
		keyWithWriteIntent, []string{key},
		preventFutureWrite,
		consts.MaxRetryResolveFoundedWriteIntent)
	if err != nil {
		return types.EmptyValue, errors.Annotatef(errors.ErrReadUncommittedData, "reason: '%v'", err)
	}
	assert.Must(writeTxn.ID.Version() == vv.Version)
	committed, rollbacked := writeTxn.CheckCommitState(ctx, txn, keyWithWriteIntent, preventFutureWrite, consts.MaxRetryResolveFoundedWriteIntent)
	if committed {
		return vv.Value, nil
	}
	if rollbacked {
		return types.EmptyValue, errors.ErrReadRollbackedData
	}
	return types.EmptyValue, errors.Annotatef(errors.ErrReadUncommittedData, "can't determine previous txn status for %d", writeTxn.ID)
}

func (txn *Txn) CheckCommitState(ctx context.Context, callerTxn *Txn, keysWithWriteIntent map[string]struct{}, preventFutureWrite bool, maxRetry int) (committed bool, rollbacked bool) {
	txn.Lock()
	defer txn.Unlock()

	return txn.checkCommitState(ctx, callerTxn, keysWithWriteIntent, preventFutureWrite, maxRetry)
}

func (txn *Txn) checkCommitState(ctx context.Context, callerTxn *Txn, keysWithWriteIntent map[string]struct{}, preventFutureWrite bool, maxRetry int) (committed bool, rollbacked bool) {
	switch txn.State {
	case types.TxnStateStaging:
		if len(txn.WrittenKeys) == 0 {
			glog.Fatalf("[checkCommitState] len(txn.WrittenKeys) == 0, txn: %v", txn)
		}
		if len(keysWithWriteIntent) == len(txn.WrittenKeys) {
			assert.MustAllContain(keysWithWriteIntent, txn.WrittenKeys)
			txn.State = types.TxnStateCommitted
			txn.onCommitted(callerTxn.ID, "found committed during CheckCommitState: len(keysWithWriteIntent) == len(txn.WrittenKeys)") // help commit since original txn coordinator may have gone
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
			vv, exists, err := txn.store.getValueWrittenByTxnWithRetry(ctx, key, txn.ID, callerTxn, preventFutureWrite, maxRetry)
			if err != nil {
				return false, false
			}
			if exists {
				assert.Must(vv.Version == txn.ID.Version())
				if !vv.HasWriteIntent() {
					txn.State = types.TxnStateCommitted
					txn.onCommitted(callerTxn.ID, fmt.Sprintf("found committed during CheckCommitState: key '%s' committed", key)) // help commit since original txn coordinator may have gone
					return true, false
				}
				continue
			}
			if idx >= notInKeysLen {
				txn.State = types.TxnStateRollbacking
				_ = txn.rollback(ctx, callerTxn.ID, true, "previous write intent disappeared") // help rollback since original txn coordinator may have gone
				return false, true
			}
			if vv.MaxReadVersion > txn.ID.Version() {
				txn.State = types.TxnStateRollbacking
				_ = txn.rollback(ctx, callerTxn.ID, true, fmt.Sprintf("found non exist key during CheckCommitState"+
					" and is impossible to succeed in the future, max read version(%d) > txnId(%d)", vv.MaxReadVersion, txn.ID)) // help rollback since original txn coordinator may have gone
				return false, true
			}
			return false, false
		}
		txn.State = types.TxnStateCommitted
		txn.onCommitted(callerTxn.ID, "found committed during CheckCommitState: all keys exist") // help commit since original txn coordinator may have gone
		return true, false
	case types.TxnStateCommitted:
		return true, false
	case types.TxnStateRollbacking:
		_ = txn.rollback(ctx, callerTxn.ID, false, fmt.Sprintf("transaction in state '%s'", types.TxnStateRollbacking)) // help rollback since original txn coordinator may have gone
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

	if txn.preventWriteFlags[key] {
		return errors.Annotatef(errors.ErrWriteReadConflict, "a transaction with higher timestamp has read the key '%s'", key)
	}

	writeVal := types.NewValue(val, txn.ID.Version())
	writeTask := types.NewListTaskNoResult(
		txn.ioTaskIDOfKey(key),
		fmt.Sprintf("set-key-%s", key),
		txn.cfg.StaleWriteThreshold, func(ctx context.Context, _ interface{}) error {

			return txn.kv.Set(ctx, key, writeVal, types.KVCCWriteOption{})
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
	for _, task := range txn.writeKeyTasks {
		// TODO this should be safe even though no sync was executed
		if err := task.ErrUnsafe(); errors.IsMustRollbackWriteKeyErr(err) {
			txn.State = types.TxnStateRollbacking
			_ = txn.rollback(ctx, txn.ID, true, fmt.Sprintf("write key io task '%s' failed: '%v'", task.ID, err.Error()))
			return err
		}
	}
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
		txn.h.RemoveTxn(txn)
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
	maxRetry := utils.MaxInt(int(int64(txn.cfg.StaleWriteThreshold)/int64(time.Second))*3, 10)
	if recordErr := txn.txnRecordTask.Err(); recordErr != nil {
		inferredTxn, err := txn.store.inferTransactionRecordWithRetry(ctx, txn.ID, txn, nil, txn.WrittenKeys, true, maxRetry)
		if err != nil {
			txn.State = types.TxnStateInvalid
			return err
		}
		assert.Must(txn.ID == inferredTxn.ID)
		assert.MustEqualStrings(inferredTxn.WrittenKeys, txn.WrittenKeys)
		txn.State = inferredTxn.State
		if txn.State == types.TxnStateCommitted {
			return nil
		}
		if txn.State.IsAborted() {
			return lastNonRollbackableIOErr
		}
	}
	assert.Must(txn.State == types.TxnStateStaging)
	succeededKeys := make(map[string]struct{})
	for key, lastWriteKeyTask := range txn.lastWriteKeyTasks {
		if lastWriteKeyTask.Err() == nil {
			succeededKeys[key] = struct{}{}
		}
	}
	assert.Must(len(succeededKeys) != len(txn.WrittenKeys) || txn.txnRecordTask.Err() != nil)
	committed, rollbacked := txn.checkCommitState(ctx, txn, succeededKeys, true, maxRetry)
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

	if callerTxn != txn.ID {
		glog.V(5).Infof("rollbacking txn %d..., callerTxn: %d, reason: '%v'", txn.ID, callerTxn, reason)
	} else {
		glog.V(10).Infof("rollbacking txn %d..., reason: '%v'", txn.ID, reason)
	}

	if txn.State != types.TxnStateUncommitted && txn.State != types.TxnStateRollbacking {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s or %s, but got %s", types.TxnStateUncommitted, types.TxnStateRollbacking, txn.State)
	}

	//txn.h.(*TransactionManager).rollbackedTxns.Set(txn.ID, txn)
	txn.State = types.TxnStateRollbacking
	for _, key := range txn.WrittenKeys {
		if removeErr := txn.kv.Set(ctx, key,
			types.NewValue(nil, txn.ID.Version()).WithNoWriteIntent(),
			types.NewKVCCWriteOption().WithRemoveVersion()); removeErr != nil && !errors.IsNotExistsErr(removeErr) {
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
			types.NewKVCCWriteOption().WithClearWriteIntent()); setErr != nil {
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
	err := txn.kv.Set(ctx, txn.Key(), types.NewValue(txn.Encode(), txn.ID.Version()), types.KVCCWriteOption{})
	if err != nil {
		glog.V(4).Infof("[writeTxnRecord] write transaction record failed: %v", err)
	}
	return err
}

func (txn *Txn) removeTxnRecord(ctx context.Context) error {
	err := txn.kv.Set(ctx, txn.Key(),
		types.NewValue(nil, txn.ID.Version()).WithNoWriteIntent(),
		types.NewKVCCWriteOption().WithRemoveVersion())
	if err != nil && !errors.IsNotExistsErr(err) {
		glog.Warningf("clear transaction record failed: %v", err)
		return err
	}
	return nil
}

func (txn *Txn) isTooStale() bool {
	return utils.IsTooStale(txn.ID.Version(), txn.cfg.StaleWriteThreshold)
}
