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
	"github.com/leisurelyrcxf/spermwhale/txn/ttypes"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/basic"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

type TransactionInfo struct {
	ID types.TxnId
	types.TxnState
	types.TxnType
	types.TxnSnapshotReadOption
}

func InvalidTransactionInfo(id types.TxnId) TransactionInfo {
	return TransactionInfo{
		ID:       id,
		TxnState: types.TxnStateInvalid,
		TxnType:  types.TxnTypeDefault,
	}
}

func NewTransactionInfoFromPB(x *txnpb.Txn) TransactionInfo {
	return TransactionInfo{
		ID:                    types.TxnId(x.Id),
		TxnState:              types.TxnState(x.State),
		TxnType:               types.TxnType(x.Type),
		TxnSnapshotReadOption: types.NewTxnSnapshotReadOptionFromPB(x.SnapshotReadOption),
	}
}

func (i TransactionInfo) ToPB() *txnpb.Txn {
	return &txnpb.Txn{
		Id:                 i.ID.Version(),
		State:              i.TxnState.ToPB(),
		Type:               i.TxnType.ToUint32(),
		SnapshotReadOption: i.TxnSnapshotReadOption.ToPB(),
	}
}

func (i *TransactionInfo) GetId() types.TxnId {
	return i.ID
}

func (i *TransactionInfo) GetState() types.TxnState {
	return i.TxnState
}

func (i *TransactionInfo) GetType() types.TxnType {
	return i.TxnType
}

func (i *TransactionInfo) GetSnapshotReadOption() types.TxnSnapshotReadOption {
	return i.TxnSnapshotReadOption
}

type Txn struct {
	sync.Mutex `json:"-"`

	TransactionInfo
	ttypes.WriteKeyInfos

	preventFutureWriteKeys  basic.Set
	readModifyWriteReadKeys basic.Set

	txnRecordTask *basic.Task

	allWriteTasksFinished bool

	cfg   types.TxnConfig
	kv    types.KVCC
	store *TransactionStore
	s     *Scheduler
	gc    func(_ *Txn, force bool)

	err error
}

func NewTxn(
	id types.TxnId, typ types.TxnType,
	kv types.KVCC, cfg types.TxnConfig,
	store *TransactionStore,
	s *Scheduler,
	destroy func(_ *Txn, force bool)) *Txn {
	return &Txn{
		TransactionInfo: TransactionInfo{
			ID:       id,
			TxnState: types.TxnStateUncommitted,
			TxnType:  typ,
		},
		WriteKeyInfos: ttypes.WriteKeyInfos{
			KV:          kv,
			TaskTimeout: cfg.WoundUncommittedTxnThreshold,
		},

		cfg:   cfg,
		kv:    kv,
		store: store,
		s:     s,
		gc:    destroy,
	}
}

type Marshaller struct {
	State       types.TxnState     `json:"S"`
	Type        types.TxnType      `json:"T"`
	WrittenKeys ttypes.KeyVersions `json:"K"`
}

func DecodeTxn(txnId types.TxnId, data []byte) (*Txn, error) {
	var t Marshaller
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, err
	}
	assert.Must(!t.State.IsStaging() || len(t.WrittenKeys) > 0)

	txn := &Txn{}
	txn.TransactionInfo = TransactionInfo{
		ID:       txnId,
		TxnState: t.State,
		TxnType:  t.Type,
	}
	txn.InitializeWrittenKeys(t.WrittenKeys, txn.IsStaging())
	return txn, nil
}

func (txn *Txn) Encode() []byte {
	assert.Must(!txn.IsStaging() || (txn.AreWrittenKeysCompleted() && txn.GetWrittenKeyCount() > 0))
	bytes, err := json.Marshal(Marshaller{
		State:       txn.TxnState,
		Type:        txn.TxnType,
		WrittenKeys: txn.GetWrittenKey2LastVersion(), // TODO does the key write order matter?
	})
	if err != nil {
		glog.Fatalf("marshal json failed: %v", err)
	}
	return bytes
}

func (txn *Txn) Get(ctx context.Context, key string) (types.TValue, error) {
	if key == "" {
		return types.EmptyTValue, errors.ErrEmptyKey
	}

	txn.Lock()
	defer txn.Unlock()

	if txn.TxnState != types.TxnStateUncommitted {
		return types.EmptyTValue, errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s, but got %s", types.TxnStateUncommitted, txn.TxnState)
	}

	if txn.IsSnapshotRead() {
		return txn.getSnapshot(ctx, key)
	}
	return txn.getLatest(ctx, key)
}

func (txn *Txn) getLatest(ctx context.Context, key string) (tVal types.TValue, err error) {
	assert.Must(!txn.IsSnapshotRead())
	defer func() {
		if err != nil {
			txn.err = err
		}
		if err != nil && errors.IsMustRollbackGetErr(err) {
			txn.TxnState = types.TxnStateRollbacking
			_ = txn.rollback(ctx, txn.ID, true, "error occurred during Txn::Get: %v", err)
		}
		if err == nil {
			tVal.AssertValid()
		}
		assert.Must(err != nil || tVal.Version == txn.ID.Version() || (tVal.Version != 0 && tVal.IsCommitted()))
	}()

	for i := 0; ; i++ {
		val, err := txn.getLatestOneRound(ctx, key)
		if err == nil || !errors.IsRetryableGetErr(err) || i >= consts.MaxRetryTxnGet-1 || ctx.Err() != nil {
			return val.ToTValue().CondPreventedFutureWrite(txn.preventFutureWriteKeys.Contains(key)), err
		}
		if errors.GetErrorCode(err) != consts.ErrCodeReadUncommittedDataPrevTxnKeyRollbacked {
			rand.Seed(time.Now().UnixNano())
			time.Sleep(time.Duration(1+rand.Intn(3)) * time.Millisecond)
		}
	}
}

func (txn *Txn) getLatestOneRound(ctx context.Context, key string) (_ types.ValueCC, err error) {
	if txn.TxnState != types.TxnStateUncommitted {
		return types.EmptyValueCC, errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s, but got %s", types.TxnStateUncommitted, txn.TxnState)
	}

	var (
		vv            types.ValueCC
		lastWriteTask = txn.GetLastWriteKeyTask(key)
	)
	if lastWriteTask != nil {
		ctx, cancel := context.WithTimeout(ctx, consts.DefaultReadTimeout)
		if !lastWriteTask.WaitFinishWithContext(ctx) {
			cancel()
			return types.EmptyValueCC, errors.Annotatef(errors.ErrReadAfterWriteFailed, "wait write task timeouted after %s", consts.DefaultReadTimeout)
		}
		cancel()
		if writeErr := lastWriteTask.Err(); writeErr != nil {
			return types.EmptyValueCC, errors.Annotatef(errors.ErrReadAfterWriteFailed, "previous error: '%v'", writeErr)
		}
		vv, err = txn.kv.Get(ctx, key, types.NewKVCCReadOption(txn.ID.Version()).WithExactVersion(txn.ID.Version()).WithNotUpdateTimestampCache()) // TODO check this is safe?
	} else {
		vv, err = txn.kv.Get(ctx, key, types.NewKVCCReadOption(txn.ID.Version()).
			CondReadModifyWrite(txn.IsReadModifyWrite()).CondReadModifyWriteFirstReadOfKey(!txn.readModifyWriteReadKeys.Contains(key)).
			CondWaitWhenReadDirty(txn.IsWaitWhenReadDirty()))
	}
	if //noinspection ALL
	vv.MaxReadVersion > txn.ID.Version() {
		txn.preventFutureWriteKeys.Insert(key)
	}
	if txn.IsReadModifyWrite() { // NOTE: Must be after CondReadModifyWriteFirstReadOfKey(!txn.readModifyWriteReadKeys.Contains(key))
		txn.readModifyWriteReadKeys.Insert(key)
	}
	if err != nil {
		if lastWriteTask != nil && errors.IsNotExistsErr(err) {
			return types.EmptyValueCC, errors.ErrReadUncommittedDataPrevTxnKeyRollbackedReadAfterWrite
		}
		return types.EmptyValueCC, err
	}
	assert.Must((lastWriteTask == nil && vv.Version < txn.ID.Version()) || (lastWriteTask != nil && vv.Version == txn.ID.Version()))
	assert.Must(!vv.IsAborted())
	if vv.IsCommitted() || vv.Version == txn.ID.Version() /* read after write */ {
		return vv, nil
	}
	assert.Must(txn.ID.Version() > vv.Version)
	var preventFutureWrite = utils.IsTooOld(vv.Version, txn.cfg.WoundUncommittedTxnThreshold)
	writeTxn, committedKey, err := txn.store.inferTransactionRecordWithRetry(
		ctx,
		types.TxnId(vv.Version), txn,
		map[string]struct{}{key: {}},
		ttypes.KeyVersions{key: types.TxnInternalVersionPositiveInvalid}, // last internal version not known, use an invalid version instead, don't use 0 because we want to guarantee WriteKeyInfo.NotEmpty()
		preventFutureWrite,
		consts.MaxRetryResolveFoundedWriteIntent)
	if err != nil {
		return types.EmptyValueCC, errors.Annotatef(errors.ErrReadUncommittedDataPrevTxnStatusUndetermined, "reason: '%v', txn: %d", err, vv.Version)
	}
	assert.Must(writeTxn.ID.Version() == vv.Version && vv.MaxReadVersion >= txn.ID.Version() /* > vv.Version */)
	writeTxn.checkCommitState(ctx, txn, map[string]types.ValueCC{key: vv}, preventFutureWrite, committedKey, consts.MaxRetryResolveFoundedWriteIntent)
	if writeTxn.IsCommitted() {
		committedTxnInternalVersion := writeTxn.GetCommittedVersion(key)
		assert.Must(committedTxnInternalVersion == 0 || committedTxnInternalVersion == vv.InternalVersion) // no way to write a new version after read
		vv.SetCommitted()
		return vv, nil
	}
	if writeTxn.IsAborted() {
		if writeTxn.GetKeyStateUnsafe(key).IsRollbackedCleared() { // TODO change dbReadVersion
			return types.EmptyValueCC, errors.ErrReadUncommittedDataPrevTxnKeyRollbacked
		}
		assert.Must(writeTxn.TxnState == types.TxnStateRollbacking)
		return types.EmptyValueCC, errors.Annotatef(errors.ErrReadUncommittedDataPrevTxnToBeRollbacked, "previous txn %d", writeTxn.ID) // TODO can be optimized, skip the aborted version
	}
	return types.EmptyValueCC, errors.Annotatef(errors.ErrReadUncommittedDataPrevTxnStatusUndetermined, "previous txn %d", writeTxn.ID)
}

func (txn *Txn) MGet(ctx context.Context, keys []string) ([]types.TValue, error) {
	if err := types.ValidateMGetRequest(keys); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, consts.DefaultReadTimeout)
	defer cancel()

	txn.Lock()
	defer txn.Unlock()

	if txn.TxnState != types.TxnStateUncommitted {
		return nil, errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s, but got %s", types.TxnStateUncommitted, txn.TxnState)
	}

	if !txn.IsSnapshotRead() {
		values := make([]types.TValue, 0, len(keys))
		for _, key := range keys {
			val, err := txn.getLatest(ctx, key)
			if err != nil {
				return nil, err
			}
			values = append(values, val)
		}
		return values, nil
	}
	return txn.mgetSnapshot(ctx, keys)
}

func (txn *Txn) checkCommitState(ctx context.Context, callerTxn *Txn, keysWithWriteIntent map[string]types.ValueCC, preventFutureWrite bool, committedKey string, maxRetry int) {
	switch txn.TxnState {
	case types.TxnStateStaging:
		assert.Must(keysWithWriteIntent != nil && txn.AreWrittenKeysCompleted() && txn.GetWrittenKeyCount() > 0)
		for key := range keysWithWriteIntent {
			assert.Must(txn.ContainsWrittenKey(key))
		}

		var (
			txnWrittenKeys = txn.GetWrittenKey2LastVersion()
		)
		if txn.ID != callerTxn.ID {
			for key, vv := range keysWithWriteIntent {
				if vv.InternalVersion != txnWrittenKeys[key] {
					assert.Must(vv.InternalVersion < txnWrittenKeys[key] && vv.MaxReadVersion > txn.ID.Version())
					txn.TxnState = types.TxnStateRollbacking
					_ = txn.rollback(ctx, callerTxn.ID, true,
						"Txn::checkCommitState (version below latest version or key '%s' not exists) and max read version(%d) > txnId(%d)", key, vv.MaxReadVersion, txn.ID) // help rollback since original txn coordinator may have gone
					return
				}
			}
		} else {
			for key, vv := range keysWithWriteIntent {
				assert.Must(vv.InternalVersion == txnWrittenKeys[key])
			}
		}
		for key := range txnWrittenKeys {
			if _, isKeyWithWriteIntent := keysWithWriteIntent[key]; isKeyWithWriteIntent {
				continue
			}
			vv, exists, notExistsErrSubCode, err := txn.store.getValueWrittenByTxnWithRetry(ctx, key, txn.ID, callerTxn, preventFutureWrite, true, maxRetry)
			assert.Must(!vv.IsAborted() || (!exists && err == nil))
			if err != nil {
				return
			}
			if exists {
				assert.Must(vv.Version == txn.ID.Version() && vv.InternalVersion > 0)
				if vv.IsCommitted() {
					txn.TxnState = types.TxnStateCommitted
					txn.MarkWrittenKeyCommitted(key, vv.Value)
					//txn.MarkWrittenKeyCommittedCleared(key, vv.Value) // TODO this is unsafe
					txn.onCommitted(callerTxn.ID, "Txn::checkCommitState key '%s' write intent cleared", key) // help commit since original txn coordinator may have gone
					return
				}
				if vv.InternalVersion == txnWrittenKeys[key] {
					continue
				}
				assert.Must(vv.InternalVersion < txnWrittenKeys[key])
			}
			// TODO evaluate the performance gain
			if vv.IsAborted() || vv.MaxReadVersion > txn.ID.Version() /* this include '!exits and preventFutureWrite' */ {
				if !exists { // not exists and won't exist
					txn.MarkWrittenKeyAborted(key, notExistsErrSubCode)
				}
				txn.TxnState = types.TxnStateRollbacking
				_ = txn.rollback(ctx, callerTxn.ID, true,
					"Txn::checkCommitState (version below latest version or key '%s' not exists) and max read version(%d) > txnId(%d)", key, vv.MaxReadVersion, txn.ID) // help rollback since original txn coordinator may have gone
				return
			}
			return // unable to determine transaction status
		}
		txn.TxnState = types.TxnStateCommitted
		txn.onCommitted(callerTxn.ID, "Txn::checkCommitState: all keys exist and match latest internal txn version") // help commit since original txn coordinator may have gone
	case types.TxnStateRollbacking:
		_ = txn.rollback(ctx, callerTxn.ID, false, "transaction rollbacking, txn error: %v", txn.err) // help rollback since original txn coordinator may have gone
	case types.TxnStateCommitted:
		txn.onCommitted(callerTxn.ID, "TransactionStore::inferTransactionRecordWithRetry: found committed key '%s'", committedKey) // help rollback since original txn coordinator may have gone
	case types.TxnStateRollbackedCleared, types.TxnStateCommittedCleared:
		txn.gc(txn, false)
	default:
		panic(fmt.Sprintf("impossible transaction state %s", txn.TxnState))
	}
}

func (txn *Txn) Set(ctx context.Context, key string, val []byte) error {
	if key == "" {
		return errors.ErrEmptyKey
	}
	if txn.IsSnapshotRead() {
		return errors.Annotatef(errors.ErrNotAllowed, "can't write in snapshot isolation level")
	}

	txn.Lock()
	defer txn.Unlock()

	if txn.TxnState != types.TxnStateUncommitted {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s, but got %s", types.TxnStateUncommitted, txn.TxnState)
	}
	return txn.set(ctx, key, val)
}

func (txn *Txn) set(ctx context.Context, key string, val []byte) error {
	if txn.preventFutureWriteKeys.Contains(key) {
		txn.err = errors.Annotatef(errors.ErrWriteReadConflict, "a transaction with higher timestamp has read the key '%s'", key)
		_ = txn.rollback(ctx, txn.ID, true, "error occurred during Txn::Set: %v", txn.err)
		return txn.err
	}

	if err := txn.WriteKey(txn.s.writeJobScheduler, key, types.NewValue(val, txn.ID.Version()),
		types.NewKVCCWriteOption().CondReadModifyWrite(txn.IsReadModifyWrite())); err != nil {
		_ = txn.rollback(ctx, txn.ID, true, "error occurred during Txn::Set: %v", txn.err)
		txn.err = err
		return err
	}
	for _, task := range txn.GetWriteKeyTasks() {
		// TODO this should be safe even though no sync was executed
		if err := task.ErrUnsafe(); errors.IsMustRollbackWriteKeyErr(err) {
			_ = txn.rollback(ctx, txn.ID, true, "Txn::Set write key io task '%s' failed: '%v'", task.ID, err)
			txn.err = err
			return err
		}
	}
	return nil
}

func (txn *Txn) MSet(ctx context.Context, keys []string, values [][]byte) error {
	if err := types.ValidateMSetRequest(keys, values); err != nil {
		return err
	}
	if txn.IsSnapshotRead() {
		return errors.Annotatef(errors.ErrNotAllowed, "can't write in snapshot isolation level")
	}

	txn.Lock()
	defer txn.Unlock()

	if txn.TxnState != types.TxnStateUncommitted {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s, but got %s", types.TxnStateUncommitted, txn.TxnState)
	}

	for idx, key := range keys {
		if err := txn.set(ctx, key, values[idx]); err != nil {
			return err
		}
	}
	return nil
}

func (txn *Txn) Commit(ctx context.Context) error {
	txn.Lock()
	defer txn.Unlock()

	if txn.TxnState != types.TxnStateUncommitted {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s, but got %s", types.TxnStateUncommitted, txn.TxnState)
	}

	if txn.GetWrittenKeyCount() == 0 {
		if txn.IsReadModifyWrite() && len(txn.readModifyWriteReadKeys) > 0 { // todo commit instead?
			_ = txn.rollback(ctx, txn.ID, false, "readModifyWrite transaction write no keys")
			return errors.ErrReadModifyWriteTransactionCommitWithNoWrittenKeys
		}
		txn.TxnState = types.TxnStateCommitted
		txn.gc(txn, true)
		return nil
	}

	txn.TxnState = types.TxnStateStaging
	txn.MarkWrittenKeysCompleted()
	recordData := txn.Encode()
	txnRecordTask := basic.NewTask(
		basic.NewTaskId(txn.ID.Version(), ""), "write-staging-txn-record",
		txn.cfg.WoundUncommittedTxnThreshold, func(ctx context.Context) (interface{}, error) {
			if err := txn.writeTxnRecord(ctx, recordData); err != nil {
				return nil, err
			}
			return basic.DummyTaskResult, nil
		})
	if err := txn.s.ScheduleWriteTxnRecordJob(txnRecordTask); err != nil {
		txn.TxnState = types.TxnStateRollbacking
		_ = txn.rollback(ctx, txn.ID, true, "write record io task schedule failed: '%v'", err)
		return err
	}
	txn.txnRecordTask = txnRecordTask

	// Wait finish
	txn.waitWriteTasks(ctx, false)

	var lastNonRollbackableIOErr error
	for _, ioTask := range txn.getAllWriteTasks() {
		if ioTaskErr := ioTask.Err(); ioTaskErr != nil {
			if errors.IsMustRollbackSetErr(ioTaskErr) {
				// write record must failed
				txn.TxnState = types.TxnStateRollbacking
				_ = txn.rollback(ctx, txn.ID, true, "write key %s returns rollbackable error: '%v'", ioTask.ID, ioTaskErr)
				return ioTaskErr
			}
			lastNonRollbackableIOErr = ioTaskErr
		}
	}

	if lastNonRollbackableIOErr == nil {
		// succeeded
		txn.TxnState = types.TxnStateCommitted
		txn.onCommitted(txn.ID, "[Txn::Commit] all keys and txn record written successfully")
		return nil
	}

	// handle other kinds of error
	ctx = context.Background()
	var (
		committedKey string
		maxRetry     = utils.MaxInt(int(int64(txn.cfg.WoundUncommittedTxnThreshold)/int64(time.Second))*3, 10)
	)
	if recordErr := txn.txnRecordTask.Err(); recordErr != nil {
		var (
			inferredTxn *Txn
			err         error
		)
		inferredTxn, committedKey, err = txn.store.inferTransactionRecordWithRetry(ctx, txn.ID, txn, nil,
			txn.GetWrittenKey2LastVersion(), true, maxRetry)
		if err != nil {
			txn.TxnState = types.TxnStateInvalid
			return err
		}
		assert.Must(txn.ID == inferredTxn.ID)
		assert.Must(inferredTxn.GetWrittenKeyCount() == txn.GetWrittenKeyCount())
		txn.TxnState = inferredTxn.TxnState
	}
	var succeededKeys map[string]types.ValueCC
	if txn.IsStaging() {
		succeededKeys = txn.GetSucceededWrittenKeysUnsafe()
		assert.Must(len(succeededKeys) != txn.GetWrittenKeyCount() || txn.txnRecordTask.Err() != nil)
	}
	txn.checkCommitState(ctx, txn, succeededKeys, true, committedKey, maxRetry)
	if txn.IsCommitted() {
		return nil
	}
	if txn.IsAborted() {
		return txn.genAbortErr(lastNonRollbackableIOErr)
	}
	// TODO user can retry later
	return lastNonRollbackableIOErr
}

func (txn *Txn) Rollback(ctx context.Context) error {
	txn.Lock()
	defer txn.Unlock()

	return txn.rollback(ctx, txn.ID, true, "rollback by user, txn err: '%v'", txn.err)
}

func (txn *Txn) rollback(ctx context.Context, callerTxn types.TxnId, createTxnRecordOnFailure bool, reason string, args ...interface{}) error {
	if callerTxn != txn.ID && !txn.couldBeWounded() {
		assert.Must(txn.TxnState == types.TxnStateRollbacking)
		return nil
	}
	txn.waitWriteTasks(ctx, true)
	if txn.TxnState == types.TxnStateRollbackedCleared {
		return nil
	}

	if txn.TxnState != types.TxnStateUncommitted && txn.TxnState != types.TxnStateRollbacking {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s or %s, but got %s", types.TxnStateUncommitted, types.TxnStateRollbacking, txn.TxnState)
	}

	assert.Must(txn.txnRecordTask == nil || txn.AreWrittenKeysCompleted())
	assert.Must(!txn.IsStaging() || txn.AreWrittenKeysCompleted())
	txn.TxnState = types.TxnStateRollbacking
	var (
		notRemoveTxnRecord = !txn.AreWrittenKeysCompleted() || (txn.ID == callerTxn && txn.txnRecordTask == nil)
		toClearKeys        = txn.getToClearKeys(true)
	)
	if notRemoveTxnRecord && len(toClearKeys) == 0 {
		if txn.ID == callerTxn || txn.AreWrittenKeysCompleted() {
			txn.TxnState = types.TxnStateRollbackedCleared
		}
		return nil
	}

	{
		var deltaV = 0
		if errors.IsQueueFullErr(txn.err) {
			deltaV += 10
		}
		if callerTxn != txn.ID {
			if glog.V(glog.Level(7 + deltaV)) {
				glog.Infof(fmt.Sprintf("rollbacking txn %d..., callerTxn: %d, reason: '%s'", txn.ID, callerTxn, reason), args...)
			}
		} else {
			if glog.V(glog.Level(20 + deltaV)) {
				glog.Infof(fmt.Sprintf("rollbacking txn %d..., reason: '%s'", txn.ID, reason), args...)
			}
		}
	}

	var (
		root = types.NewTreeTaskNoResult(
			basic.NewTaskId(txn.ID.Version(), ""), "remove-txn-record-after-rollback", txn.cfg.ClearTimeout,
			nil, func(ctx context.Context, _ []interface{}) error {
				if notRemoveTxnRecord {
					return nil
				}
				return txn.removeTxnRecord(ctx, true, callerTxn)
			},
		)
		writtenKeyChildren = make([]*types.TreeTask, 0, txn.GetWrittenKeyCount())
	)
	defer func() {
		if txn.TxnState == types.TxnStateRollbacking && createTxnRecordOnFailure && !root.ChildrenSuccess(writtenKeyChildren) {
			_ = txn.writeTxnRecord(ctx, txn.Encode()) // make life easier for other transactions // TODO needs to gc later
		}
	}()

	for key, isWrittenKey := range toClearKeys {
		var (
			key          = key
			isWrittenKey = isWrittenKey
			opt          = types.EmptyKVCCRollbackKeyOption.CondReadModifyWrite(txn.IsReadModifyWrite()).
					CondReadOnlyKey(!isWrittenKey).CondRollbackByDifferentTxn(callerTxn != txn.ID)
		)
		if child := types.NewTreeTaskNoResult(
			basic.NewTaskId(txn.ID.Version(), key), "rollback-key", txn.cfg.ClearTimeout, root, func(ctx context.Context, _ []interface{}) error {
				_ = reason
				if removeErr := txn.kv.RollbackKey(ctx, key, txn.ID.Version(), opt); removeErr != nil && !errors.IsNotExistsErr(removeErr) {
					glog.Warningf("rollback key %v failed: '%v'", key, removeErr)
					return removeErr
				}
				return nil
			},
		); isWrittenKey {
			writtenKeyChildren = append(writtenKeyChildren, child)
		}
	}

	if err := txn.s.ScheduleClearJobTree(root); err != nil {
		return err
	}
	if !root.WaitFinishWithContext(ctx) {
		createTxnRecordOnFailure = false
		return errors.Annotatef(ctx.Err(), "wait rollback root task finish timeouted")
	}
	for _, c := range writtenKeyChildren {
		if err := c.Err(); err == nil {
			txn.MarkWrittenKeyRollbackedCleared(c.ID.Key)
		}
	}
	if err := root.Err(); err != nil {
		return err
	}

	if txn.ID == callerTxn || txn.AreWrittenKeysCompleted() {
		txn.TxnState = types.TxnStateRollbackedCleared
		txn.gc(txn, true)
	}
	return nil
}

func (txn *Txn) onCommitted(callerTxn types.TxnId, reason string, args ...interface{}) { // TODO handle clear failed
	assert.Must(txn.IsCommitted() && txn.GetWrittenKeyCount() > 0)

	txn.MarkAllWrittenKeysCommitted()
	if callerTxn != txn.ID && !txn.couldBeWounded() {
		return
	}
	if txn.TxnState == types.TxnStateCommittedCleared {
		return
	}

	toClearKeys := txn.getToClearKeys(false)
	removeTxnRecord := txn.AreWrittenKeysCompleted()

	if !removeTxnRecord && len(toClearKeys) == 0 {
		return
	}

	if callerTxn != txn.ID {
		if glog.V(7) {
			glog.Infof(fmt.Sprintf("clearing committed status for stale txn %d..., callerTxn: %d, reason: '%v'", txn.ID, callerTxn, reason), args...)
		}
	} else {
		if glog.V(350) {
			glog.Infof(fmt.Sprintf("clearing committed status for stale txn %d..., reason: '%v'", txn.ID, reason), args...)
		}
		assert.Must(txn.txnRecordTask != nil)
	}

	var root *types.TreeTask
	root = types.NewTreeTaskNoResult(
		basic.NewTaskId(txn.ID.Version(), ""), "remove-txn-record-after-commit", txn.cfg.ClearTimeout,
		nil, func(ctx context.Context, _ []interface{}) error {
			if !removeTxnRecord {
				return nil
			}
			if consts.BuildOption.IsDebug() { // TODO remove this in product
				assert.Must(root.AllChildrenSuccess())
				txn.ForEachWrittenKey(func(key string, _ ttypes.WriteKeyInfo) {
					val, exists, _, err := txn.store.getValueWrittenByTxnWithRetry(ctx, key, txn.ID, txn, false, false, 1)
					if !(err == nil && exists && val.IsCommitted() && !val.IsAborted()) {
						_ = root
						glog.Fatalf("txn-%d cleared write key '%s' not exists", txn.ID, key)
					}
				})
			}
			if err := txn.removeTxnRecord(ctx, false, callerTxn); err != nil {
				return err
			}
			txn.gc(txn, true)
			return nil
		},
	)

	for key, isWrittenKey := range toClearKeys {
		var (
			key          = key
			isWrittenKey = isWrittenKey
			opt          = types.KVCCClearWriteIntent.CondReadModifyWrite(txn.IsReadModifyWrite()).
					CondReadOnlyKey(!isWrittenKey).CondUpdateByDifferentTxn(callerTxn != txn.ID).WithInternalVersion(txn.MustGetInternalVersion(key))
		)
		_ = types.NewTreeTaskNoResult(
			basic.NewTaskId(txn.ID.Version(), key), "clear-key-write-intent", txn.cfg.ClearTimeout, root, func(ctx context.Context, _ []interface{}) error {
				if clearErr := txn.kv.UpdateMeta(ctx, key, txn.ID.Version(), opt); clearErr != nil {
					if errors.IsNotExistsErr(clearErr) {
						_ = txn
						glog.Fatalf("Status corrupted: txn-%d clear transaction key '%s' write intent failed: '%v'", txn.ID, key, clearErr)
					} else {
						glog.Warningf("txn-%d clear transaction key '%s' write intent failed: '%v'", txn.ID, key, clearErr)
					}
					return clearErr
				}
				return nil
			},
		)
	}

	_ = txn.s.ScheduleClearJobTree(root)
}

func (txn *Txn) getToClearKeys(rollback bool) map[string]bool /* key->isWrittenKey */ {
	var m = make(map[string]bool)
	if txn.IsReadModifyWrite() {
		for readKey := range txn.readModifyWriteReadKeys {
			m[readKey] = false
		}
	}
	txn.ForEachWrittenKey(func(writtenKey string, info ttypes.WriteKeyInfo) {
		assert.Must(!info.IsEmpty()) // TODO should support this after bug fixed
		if !info.IsCleared() {
			m[writtenKey] = true
		} else {
			if !rollback && bool(glog.V(40)) {
				glog.Infof("[Txn::getToClearKeys] txn-%d skip committed cleared key '%s'", txn.ID, writtenKey)
			}
			assert.Must(info.IsAborted() == rollback)
		}
	})
	return m
}

func (txn *Txn) waitWriteTasks(ctx context.Context, forceCancel bool) {
	if txn.allWriteTasksFinished {
		return
	}

	var (
		writeTasks, writeListTasks = txn.GetCopiedWriteKeyTasksEx()
		lastWriteTasks             = txn.GetLastWriteKeyTasks(writeListTasks)
		cancelledTasks             []*basic.Task
	)
	if txn.txnRecordTask != nil {
		writeTasks = append(writeTasks, txn.txnRecordTask)
	}
	if len(writeTasks) == 0 {
		return
	}
	var ctxErr = ctx.Err()
	if ctxErr != nil || forceCancel {
		cancelledTasks = make([]*basic.Task, 0, len(writeTasks))
	}
	for _, writeTask := range writeTasks {
		if ctxErr == nil && !forceCancel {
			if !writeTask.WaitFinishWithContext(ctx) {
				writeTask.Cancel()
				cancelledTasks = append(cancelledTasks, writeTask)
				ctxErr = ctx.Err()
			}
		} else {
			writeTask.Cancel()
			cancelledTasks = append(cancelledTasks, writeTask)
		}
	}
	for _, cancelledTask := range cancelledTasks {
		cancelledTask.WaitFinish()
	}
	for _, writeTask := range writeTasks {
		assert.Must(writeTask.Finished()) // TODO remove this in product
	}

	txn.s.GCWriteJobs(lastWriteTasks)
	txn.allWriteTasksFinished = true
}

func (txn *Txn) getAllWriteTasks() []*basic.Task {
	writeTasks := txn.GetCopiedWriteKeyTasks()
	if txn.txnRecordTask != nil {
		writeTasks = append(writeTasks, txn.txnRecordTask)
	}
	return writeTasks
}

func (txn *Txn) writeTxnRecord(ctx context.Context, recordData []byte) error {
	// set write intent so that other transactions can stop this txn from committing,
	// thus implement the safe-rollback functionality
	err := txn.kv.Set(ctx, "", types.NewTxnValue(recordData, txn.ID.Version()).WithInternalVersion(types.TxnInternalVersionMin), types.NewKVCCWriteOption())
	if err != nil {
		glog.V(7).Infof("[writeTxnRecord] write transaction record failed: %v", err)
	}
	return err
}

func (txn *Txn) removeTxnRecord(ctx context.Context, rollback bool, callerTxn types.TxnId) error {
	if err := txn.kv.RemoveTxnRecord(ctx, txn.ID.Version(),
		types.EmptyKVCCRemoveTxnRecordOption.CondRollback(rollback).CondRemoveByDifferentTransaction(txn.ID != callerTxn)); err != nil && !errors.IsNotExistsErr(err) {
		glog.Warningf("clear transaction record failed: %v", err)
		return err
	}
	return nil
}

func (txn *Txn) couldBeWounded() bool {
	return utils.IsTooOld(txn.ID.Version(), txn.cfg.WoundUncommittedTxnThreshold)
}

func (txn *Txn) GetReadValues() map[string]types.TValue {
	panic(errors.ErrNotSupported)
}

func (txn *Txn) GetWriteValues() map[string]types.Value {
	panic(errors.ErrNotSupported)
}

func (txn *Txn) genAbortErr(reason error) error {
	assert.Must(txn.IsAborted())
	if txn.TxnState == types.TxnStateRollbacking {
		return errors.Annotatef(errors.ErrTxnRollbacking, "reason: '%v'", reason.Error())
	}
	return errors.Annotatef(errors.ErrTxnRollbacked, "reason: '%v'", reason.Error())
}

func (txn *Txn) setErr(err *errors.Error) *Txn {
	txn.err = err
	return txn
}
