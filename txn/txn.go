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
	SnapshotVersion uint64
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
		ID:              types.TxnId(x.Id),
		TxnState:        types.TxnState(x.State),
		TxnType:         types.TxnType(x.Type),
		SnapshotVersion: x.SnapshotVersion,
	}
}

func (i TransactionInfo) ToPB() *txnpb.Txn {
	return &txnpb.Txn{
		Id:              i.ID.Version(),
		State:           i.TxnState.ToPB(),
		Type:            i.TxnType.ToPB(),
		SnapshotVersion: i.SnapshotVersion,
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

func (i *TransactionInfo) InitializeSnapshotVersion() {
	if i.SnapshotVersion == 0 {
		i.SnapshotVersion = i.ID.Version()
	}
}

func (i *TransactionInfo) GetSnapshotVersion() uint64 {
	return i.SnapshotVersion
}

func (i *TransactionInfo) GetSnapshotReadOption() types.KVCCReadOption {
	return types.NewKVCCReadOption(0).WithSnapshotRead(i.SnapshotVersion)
}

type Txn struct {
	sync.Mutex `json:"-"`

	TransactionInfo
	ttypes.WriteKeyInfos

	preventWriteFlags    map[string]bool
	readForWriteReadKeys map[string]struct{}

	txnRecordTask *basic.Task

	allWriteTasksFinished bool

	cfg     types.TxnManagerConfig
	kv      types.KVCC
	store   *TransactionStore
	s       *Scheduler
	destroy func(*Txn)

	err error
}

func NewTxn(
	id types.TxnId, typ types.TxnType,
	kv types.KVCC, cfg types.TxnManagerConfig,
	store *TransactionStore,
	s *Scheduler,
	destroy func(*Txn)) *Txn {
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

		cfg:     cfg,
		kv:      kv,
		store:   store,
		s:       s,
		destroy: destroy,
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

func (txn *Txn) Get(ctx context.Context, key string, opt types.TxnReadOption) (_ types.Value, err error) {
	if key == "" {
		return types.EmptyValue, errors.ErrEmptyKey
	}
	txn.Lock()
	defer txn.Unlock()

	return txn.getLatest(ctx, key, opt)
}

func (txn *Txn) getLatest(ctx context.Context, key string, opt types.TxnReadOption) (_ types.Value, err error) {
	if txn.IsSnapshotRead() {
		return types.EmptyValue, errors.Annotatef(errors.ErrNotSupported, "call Txn::getLatest() with snapshot_read txn type")
	}
	defer func() {
		if err != nil {
			txn.err = err
		}
		if err != nil && errors.IsMustRollbackGetErr(err) {
			txn.TxnState = types.TxnStateRollbacking
			_ = txn.rollback(ctx, txn.ID, true, "error occurred during Txn::Get: %v", err)
		}
	}()

	for i := 0; ; i++ {
		val, err := txn.get(ctx, key, types.NewKVCCReadOption(txn.ID.Version()).InheritTxnReadOption(opt).
			CondReadForWrite(txn.IsReadForWrite()).CondReadForWriteFirstReadOfKey(!utils.Contains(txn.readForWriteReadKeys, key)))
		if err == nil || !errors.IsRetryableGetErr(err) || i >= consts.MaxRetryTxnGet-1 || ctx.Err() != nil {
			return val.Value, err
		}
		if errors.GetErrorCode(err) != consts.ErrCodeReadUncommittedDataPrevTxnKeyRollbacked {
			rand.Seed(time.Now().UnixNano())
			time.Sleep(time.Duration(1+rand.Intn(3)) * time.Millisecond)
		}
	}
}

func (txn *Txn) get(ctx context.Context, key string, readOpt types.KVCCReadOption) (_ types.ValueCC, err error) {
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
	}
	if txn.IsReadForWrite() {
		if txn.readForWriteReadKeys == nil {
			txn.readForWriteReadKeys = make(map[string]struct{})
		}
		txn.readForWriteReadKeys[key] = struct{}{}
	}
	if lastWriteTask == nil {
		vv, err = txn.kv.Get(ctx, key, readOpt)
	} else {
		vv, err = txn.kv.Get(ctx, key, readOpt.WithExactVersion(txn.ID.Version()).WithClearWaitNoWriteIntent())
	}
	if //noinspection ALL
	vv.MaxReadVersion > txn.ID.Version() {
		if txn.preventWriteFlags == nil {
			txn.preventWriteFlags = map[string]bool{key: true}
		} else {
			txn.preventWriteFlags[key] = true
		}
	}
	if err != nil {
		if lastWriteTask != nil && errors.IsNotExistsErr(err) {
			return types.EmptyValueCC, errors.Annotatef(errors.ErrWriteReadConflict, "reason: previous write intent disappeared probably rollbacked")
		}
		return types.EmptyValueCC, err
	}
	assert.Must((lastWriteTask == nil && vv.Version < txn.ID.Version()) || (lastWriteTask != nil && vv.Version == txn.ID.Version()))
	if !vv.Meta.HasWriteIntent() /* committed value */ ||
		vv.Version == txn.ID.Version() /* read after write */ {
		return vv, nil
	}
	assert.Must(!readOpt.IsSnapshotRead())
	assert.Must(txn.ID.Version() > vv.Version)
	var preventFutureWrite = utils.IsTooOld(vv.Version, txn.cfg.WoundUncommittedTxnThreshold)
	writeTxn, err := txn.store.inferTransactionRecordWithRetry(
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
	writeTxn.checkCommitState(ctx, txn, map[string]types.ValueCC{key: vv}, preventFutureWrite, consts.MaxRetryResolveFoundedWriteIntent)
	if writeTxn.IsCommitted() {
		committedTxnInternalVersion := writeTxn.GetCommittedVersion(key)
		assert.Must(committedTxnInternalVersion == 0 || committedTxnInternalVersion == vv.InternalVersion) // no way to write a new version after read
		return vv, nil
	}
	if writeTxn.IsAborted() {
		if writeTxn.IsWrittenKeyRollbacked(key) {
			return types.EmptyValueCC, errors.ErrReadUncommittedDataPrevTxnKeyRollbacked
		}
		assert.Must(writeTxn.TxnState == types.TxnStateRollbacking)
		return types.EmptyValueCC, errors.Annotatef(errors.ErrReadUncommittedDataPrevTxnToBeRollbacked, "previous txn %d", writeTxn.ID)
	}
	return types.EmptyValueCC, errors.Annotatef(errors.ErrReadUncommittedDataPrevTxnStatusUndetermined, "previous txn %d", writeTxn.ID)
}

func (txn *Txn) MGet(ctx context.Context, keys []string, opt types.TxnReadOption) (_ []types.Value, err error) {
	if len(keys) == 0 {
		return nil, errors.ErrEmptyKeys
	}
	txn.Lock()
	defer txn.Unlock()

	if !txn.IsSnapshotRead() {
		values := make([]types.Value, 0, len(keys))
		for _, key := range keys {
			val, err := txn.getLatest(ctx, key, opt)
			if err != nil {
				return nil, err
			}
			values = append(values, val)
		}
		return values, nil
	}

	//values := make([]types.ValueCC, 0, len(keys))
	//var snapshotVersion = txn.ID.Version()
	//for idx, key := range keys {
	//	val, err := txn.get(ctx, key, types.NewKVCCReadOption(snapshotVersion).WithSnapshotRead())
	//	if err != nil {
	//		return nil, err
	//	}
	//	assert.Must(!val.HasWriteIntent() && val.SnapshotVersion <= snapshotVersion)
	//	values = append(values, val)
	//	snapshotVersion = utils.MinUint64(snapshotVersion, val.SnapshotVersion)
	//	for j := 0; j < idx; j++ {
	//		if snapshotVersion < values[j].Version {
	//			val, err := txn.get(ctx, keys[j], types.NewKVCCReadOption(snapshotVersion).WithSnapshotRead())
	//			if err != nil {
	//				return nil, err
	//			}
	//			assert.Must(!val.HasWriteIntent() && val.SnapshotVersion <= snapshotVersion)
	//			values[j] = val
	//		}
	//	}
	//}

	txn.InitializeSnapshotVersion()
	ctx, cancel := context.WithTimeout(ctx, consts.DefaultReadTimeout)
	defer cancel()

	values, err := txn.parallelRead(ctx, keys, txn.GetSnapshotReadOption())
	if err != nil {
		return nil, err
	}

	for _, val := range values {
		txn.SnapshotVersion = utils.MinUint64(txn.SnapshotVersion, val.SnapshotVersion)
	}

	var (
		needRereadKeys       []string
		needRereadKeyIndexes []int
	)
	for idx, value := range values {
		assert.Must(!value.HasWriteIntent())
		if value.Version > txn.SnapshotVersion {
			needRereadKeys = append(needRereadKeys, keys[idx])
			needRereadKeyIndexes = append(needRereadKeyIndexes, idx)
		} else {
			//if consts.BuildOption.IsDebug() {
			//	// TODO remove this
			//	assert.Must(value.SnapshotVersion >= txn.SnapshotVersion)
			//	val, err := txn.kv.Get(ctx, keys[idx], txn.GetSnapshotReadOption())
			//	assert.MustNoError(err)
			//	assert.Must(val.Version == values[idx].Version)
			//	assert.Must(val.SnapshotVersion == txn.SnapshotVersion)
			//}
			values[idx].SnapshotVersion = txn.SnapshotVersion
		}
	}

	if len(needRereadKeys) == 0 {
		return types.ValueCCs(values).ToValues(), nil
	}

	assert.Must(txn.SnapshotVersion < txn.ID.Version())
	rereadValues, err := txn.parallelRead(ctx, needRereadKeys, txn.GetSnapshotReadOption())
	if err != nil {
		return nil, err
	}
	for rereadIdx, rereadValue := range rereadValues {
		values[needRereadKeyIndexes[rereadIdx]] = rereadValue
	}
	return types.ValueCCs(values).ToValues(), nil
}

func (txn *Txn) parallelRead(ctx context.Context, keys []string, readOpt types.KVCCReadOption) ([]types.ValueCC, error) {
	if len(keys) == 1 {
		val, err := txn.kv.Get(ctx, keys[0], readOpt)
		if err != nil {
			return nil, err
		}
		return []types.ValueCC{val}, nil
	}

	var tasks = make([]*basic.Task, 0, len(keys))
	for _, key := range keys {
		var key = key
		task := basic.NewTask(basic.NewTaskId(txn.ID.Version(), key), "get", consts.DefaultReadTimeout, func(ctx context.Context) (i interface{}, err error) {
			return txn.kv.Get(ctx, key, readOpt)
		})
		if err := txn.s.ScheduleReadJob(task); err != nil {
			for _, t := range tasks {
				t.Cancel()
			}
			return nil, err
		}
		tasks = append(tasks, task)
	}

	for _, task := range tasks {
		if !task.WaitFinishWithContext(ctx) {
			for _, t := range tasks {
				t.Cancel()
			}
			return nil, ctx.Err()
		}
	}

	values := make([]types.ValueCC, 0, len(keys))
	for _, task := range tasks {
		if err := task.Err(); err != nil {
			return nil, err
		}
		values = append(values, task.Result().(types.ValueCC))
	}
	return values, nil
}

func (txn *Txn) checkCommitState(ctx context.Context, callerTxn *Txn, keysWithWriteIntent map[string]types.ValueCC, preventFutureWrite bool, maxRetry int) {
	switch txn.TxnState {
	case types.TxnStateStaging:
		assert.Must(txn.AreWrittenKeysCompleted() && txn.GetWrittenKeyCount() > 0)
		for key := range keysWithWriteIntent {
			assert.Must(txn.ContainsWrittenKey(key))
		}
		//if txn.GetWrittenKeyCount() == len(keysWithWriteIntent) && txn.MatchWrittenKeys(keysWithWriteIntent) {
		//	txn.State = types.TxnStateCommitted
		//	txn.onCommitted(callerTxn.ID, "[CheckCommitState] txn.GetWrittenKeyCount() == len(keysWithWriteIntent) && txn.MatchWrittenKeys(keysWithWriteIntent)") // help commit since original txn coordinator may have gone
		//	return true, false
		//}

		var (
			txnWrittenKeys = txn.GetWrittenKey2LastVersion()
		)
		if txn.ID != callerTxn.ID {
			for key, vv := range keysWithWriteIntent {
				if vv.InternalVersion != txnWrittenKeys[key] {
					assert.Must(vv.InternalVersion < txnWrittenKeys[key] && vv.MaxReadVersion > txn.ID.Version())
					txn.TxnState = types.TxnStateRollbacking
					_ = txn.rollback(ctx, callerTxn.ID, true,
						"Txn::CheckCommitState (version below latest version or key '%s' not exists) and max read version(%d) > txnId(%d)", key, vv.MaxReadVersion, txn.ID) // help rollback since original txn coordinator may have gone
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
			vv, exists, err := txn.store.getValueWrittenByTxnWithRetry(ctx, key, txn.ID, callerTxn, preventFutureWrite, true, maxRetry)
			if err != nil {
				return
			}
			if exists {
				assert.Must(vv.Version == txn.ID.Version() && vv.InternalVersion > 0)
				if !vv.HasWriteIntent() {
					txn.TxnState = types.TxnStateCommitted
					txn.MarkCommittedCleared(key, vv.Value)
					txn.onCommitted(callerTxn.ID, "[CheckCommitState] key '%s' write intent cleared", key) // help commit since original txn coordinator may have gone
					return
				}
				if vv.InternalVersion == txnWrittenKeys[key] {
					continue
				}
				assert.Must(vv.InternalVersion < txnWrittenKeys[key])
			}
			if vv.MaxReadVersion > txn.ID.Version() /* this include '!exits and preventFutureWrite' */ {
				if !exists { // not exists and won't exist
					txn.MarkWrittenKeyRollbacked(key)
				}
				txn.TxnState = types.TxnStateRollbacking
				_ = txn.rollback(ctx, callerTxn.ID, true,
					"Txn::CheckCommitState (version below latest version or key '%s' not exists) and max read version(%d) > txnId(%d)", key, vv.MaxReadVersion, txn.ID) // help rollback since original txn coordinator may have gone
				return
			}
			return // unable to determine transaction status
		}
		txn.TxnState = types.TxnStateCommitted
		txn.onCommitted(callerTxn.ID, "[CheckCommitState] all keys exist and match latest internal txn version") // help commit since original txn coordinator may have gone
		return
	case types.TxnStateRollbacking:
		_ = txn.rollback(ctx, callerTxn.ID, false, "transaction rollbacking, txn error: %v", txn.err) // help rollback since original txn coordinator may have gone
		return
	case types.TxnStateRollbacked, types.TxnStateCommitted:
		return
	default:
		panic(fmt.Sprintf("impossible transaction state %s", txn.TxnState))
	}
}

func (txn *Txn) Set(ctx context.Context, key string, val []byte) error {
	if key == "" {
		return errors.ErrEmptyKey
	}

	txn.Lock()
	defer txn.Unlock()

	if txn.TxnState != types.TxnStateUncommitted {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s, but got %s", types.TxnStateUncommitted, txn.TxnState)
	}

	if txn.preventWriteFlags[key] {
		txn.err = errors.Annotatef(errors.ErrWriteReadConflict, "a transaction with higher timestamp has read the key '%s'", key)
		_ = txn.rollback(ctx, txn.ID, true, "error occurred during Txn::Set: %v", txn.err)
		return txn.err
	}

	if err := txn.WriteKey(txn.s.writeJobScheduler, key, types.NewValue(val, txn.ID.Version()),
		types.NewKVCCWriteOption().CondReadForWrite(txn.IsReadForWrite())); err != nil {
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

func (txn *Txn) Commit(ctx context.Context) error {
	txn.Lock()
	defer txn.Unlock()

	if txn.TxnState != types.TxnStateUncommitted {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s, but got %s", types.TxnStateUncommitted, txn.TxnState)
	}

	if txn.GetWrittenKeyCount() == 0 {
		if txn.IsReadForWrite() && len(txn.readForWriteReadKeys) > 0 { // todo commit instead?
			_ = txn.rollback(ctx, txn.ID, false, "readForWrite transaction write no keys")
			return errors.ErrReadForWriteTransactionCommitWithNoWrittenKeys
		}
		txn.TxnState = types.TxnStateCommitted
		txn.gc()
		return nil
	}

	txn.MarkWrittenKeyCompleted()
	txn.TxnState = types.TxnStateStaging
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
			if errors.IsMustRollbackCommitErr(ioTaskErr) {
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
	maxRetry := utils.MaxInt(int(int64(txn.cfg.WoundUncommittedTxnThreshold)/int64(time.Second))*3, 10)
	if recordErr := txn.txnRecordTask.Err(); recordErr != nil {
		inferredTxn, err := txn.store.inferTransactionRecordWithRetry(ctx, txn.ID, txn, nil,
			txn.GetWrittenKey2LastVersion(), true, maxRetry)
		if err != nil {
			txn.TxnState = types.TxnStateInvalid
			return err
		}
		assert.Must(txn.ID == inferredTxn.ID)
		assert.Must(inferredTxn.GetWrittenKeyCount() == txn.GetWrittenKeyCount())
		txn.TxnState = inferredTxn.TxnState
		if txn.IsCommitted() {
			return nil
		}
		if txn.IsAborted() {
			return txn.genAbortErr(lastNonRollbackableIOErr)
		}
	}
	assert.Must(txn.IsStaging())
	succeededKeys := txn.GetSucceededWrittenKeysUnsafe()
	assert.Must(len(succeededKeys) != txn.GetWrittenKeyCount() || txn.txnRecordTask.Err() != nil)
	txn.checkCommitState(ctx, txn, succeededKeys, true, maxRetry)
	if txn.IsCommitted() {
		return nil
	}
	if txn.IsAborted() {
		return txn.genAbortErr(lastNonRollbackableIOErr)
	}
	txn.TxnState = types.TxnStateInvalid
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
	if txn.TxnState == types.TxnStateRollbacked {
		return nil
	}

	if txn.TxnState != types.TxnStateUncommitted && txn.TxnState != types.TxnStateRollbacking {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s or %s, but got %s", types.TxnStateUncommitted, types.TxnStateRollbacking, txn.TxnState)
	}

	assert.Must(txn.txnRecordTask == nil || txn.AreWrittenKeysCompleted())
	assert.Must(!txn.IsStaging() || txn.AreWrittenKeysCompleted())
	txn.TxnState = types.TxnStateRollbacking
	var (
		noNeedToRemoveTxnRecord = !txn.AreWrittenKeysCompleted() || (txn.ID == callerTxn && txn.txnRecordTask == nil)
		toClearKeys             = txn.getToClearKeys(true)
	)
	if noNeedToRemoveTxnRecord && len(toClearKeys) == 0 {
		if txn.ID == callerTxn || txn.AreWrittenKeysCompleted() {
			txn.TxnState = types.TxnStateRollbacked
		}
		return nil
	}

	{
		var deltaV = 0
		if errors.IsQueueFullErr(txn.err) {
			deltaV += 10
		}
		var v glog.Verbose
		if callerTxn != txn.ID {
			v = glog.V(glog.Level(7 + deltaV))
		} else {
			v = glog.V(glog.Level(20 + deltaV))
		}
		if v {
			glog.Infof(fmt.Sprintf("rollbacking txn %d..., callerTxn: %d, reason: '%s'", txn.ID, callerTxn, reason), args...)
		}
	}

	var (
		root = types.NewTreeTaskNoResult(
			basic.NewTaskId(txn.ID.Version(), ""), "remove-txn-record-after-rollback", txn.cfg.ClearTimeout,
			nil, func(ctx context.Context, _ []interface{}) error {
				if noNeedToRemoveTxnRecord {
					return nil
				}
				return txn.removeTxnRecord(ctx, true)
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
			val          = types.NewValue(nil, txn.ID.Version()).WithNoWriteIntent()
			opt          = types.NewKVCCWriteOption().WithRollbackVersion().
					CondReadForWrite(txn.IsReadForWrite()).CondReadForWriteRollbackOrClearReadKey(!isWrittenKey).
					CondWriteByDifferentTransaction(callerTxn != txn.ID)
		)
		if child := types.NewTreeTaskNoResult(
			basic.NewTaskId(txn.ID.Version(), key), "rollback-key", txn.cfg.ClearTimeout, root, func(ctx context.Context, _ []interface{}) error {
				_ = reason
				if removeErr := txn.kv.Set(ctx, key, val, opt); removeErr != nil && !errors.IsNotExistsErr(removeErr) {
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
			txn.MarkWrittenKeyRollbacked(c.ID.Key)
		}
	}
	if err := root.Err(); err != nil {
		return err
	}

	if txn.ID == callerTxn || txn.AreWrittenKeysCompleted() {
		txn.TxnState = types.TxnStateRollbacked
		if callerTxn == txn.ID {
			txn.gc()
		}
	}
	return nil
}

func (txn *Txn) onCommitted(callerTxn types.TxnId, reason string, args ...interface{}) {
	assert.Must(txn.IsCommitted() && txn.AreWrittenKeysCompleted() && txn.GetWrittenKeyCount() > 0)

	txn.MarkAllWrittenKeysCommitted()
	if callerTxn != txn.ID && !txn.couldBeWounded() {
		return
	}

	if callerTxn != txn.ID {
		if glog.V(7) {
			glog.Infof(fmt.Sprintf("clearing committed status for stale txn %d..., callerTxn: %d, reason: '%v'", txn.ID, callerTxn, reason), args...)
		}
	} else {
		assert.Must(txn.txnRecordTask != nil)
	}

	var root *types.TreeTask
	root = types.NewTreeTaskNoResult(
		basic.NewTaskId(txn.ID.Version(), ""), "remove-txn-record-after-commit", txn.cfg.ClearTimeout,
		nil, func(ctx context.Context, _ []interface{}) error {
			if consts.BuildOption.IsDebug() { // TODO remove this in product
				assert.Must(root.AllChildrenSuccess())
				txn.ForEachWrittenKey(func(key string, _ ttypes.WriteKeyInfo) {
					val, exists, err := txn.store.getValueWrittenByTxnWithRetry(ctx, key, txn.ID, txn, false, false, 1)
					if !(err == nil && (exists && !val.HasWriteIntent())) {
						glog.Fatalf("txn-%d cleared write key '%s' not exists", txn.ID, key)
					}
				})
			}
			if err := txn.removeTxnRecord(ctx, false); err != nil {
				return err
			}
			if callerTxn == txn.ID {
				txn.gc()
			}
			return nil
		},
	)

	for key, isWrittenKey := range txn.getToClearKeys(false) {
		var (
			key          = key
			isWrittenKey = isWrittenKey
			val          = types.NewValue(nil, txn.ID.Version()).WithNoWriteIntent()
			opt          = types.NewKVCCWriteOption().WithClearWriteIntent().
					CondReadForWrite(txn.IsReadForWrite()).CondReadForWriteRollbackOrClearReadKey(!isWrittenKey).
					CondWriteByDifferentTransaction(callerTxn != txn.ID)
		)
		_ = types.NewTreeTaskNoResult(
			basic.NewTaskId(txn.ID.Version(), key), "clear-key-write-intent", txn.cfg.ClearTimeout, root, func(ctx context.Context, _ []interface{}) error {
				if clearErr := txn.kv.Set(ctx, key, val, opt); clearErr != nil {
					if errors.IsNotExistsErr(clearErr) {
						glog.Fatalf("Status corrupted: clear transaction key '%s' write intent failed: '%v'", key, clearErr)
					} else {
						glog.Warningf("clear transaction key '%s' write intent failed: '%v'", key, clearErr)
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
	if txn.IsReadForWrite() {
		for readKey := range txn.readForWriteReadKeys {
			m[readKey] = false
		}
	}
	txn.ForEachWrittenKey(func(writtenKey string, info ttypes.WriteKeyInfo) {
		assert.Must(!info.IsEmpty())
		if (rollback && !info.Rollbacked) || (!rollback && !info.CommittedCleared) {
			assert.Must((rollback && !info.CommittedCleared && !info.Committed) || (!rollback && !info.Rollbacked))
			m[writtenKey] = true
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
	err := txn.kv.Set(ctx, "", types.NewValue(recordData, txn.ID.Version()), types.NewKVCCWriteOption().WithTxnRecord())
	if err != nil {
		glog.V(7).Infof("[writeTxnRecord] write transaction record failed: %v", err)
	}
	return err
}

func (txn *Txn) removeTxnRecord(ctx context.Context, rollback bool) error {
	if err := txn.kv.Set(ctx, "", types.NewValue(nil, txn.ID.Version()).WithNoWriteIntent(),
		types.NewKVCCWriteOption().WithRemoveTxnRecordCondRollback(rollback)); err != nil && !errors.IsNotExistsErr(err) {
		glog.Warningf("clear transaction record failed: %v", err)
		return err
	}
	return nil
}

func (txn *Txn) gc() {
	txn.destroy(txn)
}

func (txn *Txn) couldBeWounded() bool {
	return utils.IsTooOld(txn.ID.Version(), txn.cfg.WoundUncommittedTxnThreshold)
}

func (txn *Txn) GetReadValues() map[string]types.Value {
	return types.InvalidReadValues
}

func (txn *Txn) GetWriteValues() map[string]types.Value {
	return types.InvalidWriteValues
}

func (txn *Txn) genAbortErr(reason error) error {
	assert.Must(txn.IsAborted())
	if txn.TxnState == types.TxnStateRollbacking {
		return errors.Annotatef(errors.ErrTxnRollbacking, "reason: '%v'", reason.Error())
	}
	return errors.Annotatef(errors.ErrTxnRollbacked, "reason: '%v'", reason.Error())
}
