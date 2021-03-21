package txn

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/leisurelyrcxf/spermwhale/types/basic"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/proto/txnpb"
	"github.com/leisurelyrcxf/spermwhale/txn/ttypes"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

func TransactionKey(id types.TxnId) string {
	return fmt.Sprintf("txn_%d", id)
}

type TransactionInfo struct {
	ID    types.TxnId
	State types.TxnState
	Type  types.TxnType
}

func InvalidTransactionInfo(id types.TxnId) TransactionInfo {
	return TransactionInfo{
		ID:    id,
		State: types.TxnStateInvalid,
		Type:  types.TxnTypeDefault,
	}
}

func NewTransactionInfoFromPB(x *txnpb.Txn) TransactionInfo {
	return TransactionInfo{
		ID:    types.TxnId(x.Id),
		State: types.TxnState(x.State),
		Type:  types.TxnType(x.Type),
	}
}

func (i TransactionInfo) ToPB() *txnpb.Txn {
	return &txnpb.Txn{
		Id:    i.ID.Version(),
		State: i.State.ToPB(),
		Type:  i.Type.ToPB(),
	}
}

func (i *TransactionInfo) GetId() types.TxnId {
	return i.ID
}

func (i *TransactionInfo) GetState() types.TxnState {
	return i.State
}

func (i *TransactionInfo) GetType() types.TxnType {
	return i.Type
}

type Txn struct {
	sync.Mutex `json:"-"`

	TransactionInfo
	ttypes.WriteKeyInfos

	preventWriteFlags    map[string]bool
	readForWriteReadKeys map[string]struct{}

	txnKey        string
	txnRecordTask *types.ListTask

	allIOTasksFinished bool

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
			ID:    id,
			State: types.TxnStateUncommitted,
			Type:  typ,
		},
		WriteKeyInfos: ttypes.WriteKeyInfos{
			KV:          kv,
			TaskTimeout: cfg.WoundUncommittedTxnThreshold,
		},

		txnKey: TransactionKey(id),

		cfg:     cfg,
		kv:      kv,
		store:   store,
		s:       s,
		destroy: destroy,
	}
}

// TODO don't add txn id
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
	txn := &Txn{}
	txn.TransactionInfo = TransactionInfo{
		ID:    txnId,
		State: t.State,
		Type:  t.Type,
	}
	txn.InitializeWrittenKeys(t.WrittenKeys, txn.State == types.TxnStateStaging)
	return txn, nil
}

func (txn *Txn) Encode() []byte {
	assert.Must(txn.State != types.TxnStateStaging || txn.AreWrittenKeysCompleted())
	bytes, err := json.Marshal(Marshaller{
		State:       txn.State,
		Type:        txn.Type,
		WrittenKeys: txn.GetWrittenKey2LastVersion(), // TODO does the key write order matter?
	})
	if err != nil {
		glog.Fatalf("marshal json failed: %v", err)
	}
	return bytes
}

func (txn *Txn) GenIDOfKey(key string) string {
	return fmt.Sprintf("%s-%s", txn.txnKey, key)
}

func (txn *Txn) RetrieveKeyFromID(keyId string) string {
	return keyId[len(txn.txnKey)+1:]
}

func (txn *Txn) Get(ctx context.Context, key string, opt types.TxnReadOption) (_ types.Value, err error) {
	if key == "" {
		return types.EmptyValue, errors.ErrEmptyKey
	}

	txn.Lock()
	defer txn.Unlock()

	defer func() {
		if err != nil {
			txn.err = err
		}
		if err != nil && errors.IsMustRollbackGetErr(err) {
			txn.State = types.TxnStateRollbacking
			_ = txn.rollback(ctx, txn.ID, true, "error occurred during Txn::Get: %v", err)
		}
	}()

	for i := 0; ; i++ {
		val, err := txn.get(ctx, key, opt)
		if err == nil || !errors.IsRetryableGetErr(err) || i >= consts.MaxRetryTxnGet-1 || ctx.Err() != nil {
			return val, err
		}
		if errors.GetErrorCode(err) != consts.ErrCodeReadUncommittedDataPrevTxnKeyRollbacked {
			rand.Seed(time.Now().UnixNano())
			time.Sleep(time.Duration(1+rand.Intn(3)) * time.Millisecond)
		}
	}
}

func (txn *Txn) get(ctx context.Context, key string, opt types.TxnReadOption) (_ types.Value, err error) {
	if txn.State != types.TxnStateUncommitted {
		return types.EmptyValue, errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s, but got %s", types.TxnStateUncommitted, txn.State)
	}

	var (
		vv            types.ValueCC
		lastWriteTask = txn.GetLastWriteKeyTask(key)
	)
	if lastWriteTask != nil {
		ctx, cancel := context.WithTimeout(ctx, consts.DefaultReadTimeout)
		if !lastWriteTask.WaitFinishWithContext(ctx) {
			cancel()
			return types.EmptyValue, errors.Annotatef(errors.ErrReadAfterWriteFailed, "wait write task timeouted after %s", consts.DefaultReadTimeout)
		}
		cancel()
		if writeErr := lastWriteTask.Err(); writeErr != nil {
			return types.EmptyValue, errors.Annotatef(errors.ErrReadAfterWriteFailed, "previous error: '%v'", writeErr)
		}
	}
	var readOpt = types.NewKVCCReadOption(txn.ID.Version()).InheritTxnReadOption(opt).CondReadForWrite(txn.Type.IsReadForWrite()).
		CondReadForWriteFirstReadOfKey(!utils.Contains(txn.readForWriteReadKeys, key))
	if txn.Type.IsReadForWrite() {
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
			return types.EmptyValue, errors.Annotatef(errors.ErrWriteReadConflict, "reason: previous write intent disappeared probably rollbacked")
		}
		return types.EmptyValue, err
	}
	assert.Must((lastWriteTask == nil && vv.Version < txn.ID.Version()) || (lastWriteTask != nil && vv.Version == txn.ID.Version()))
	if !vv.Meta.HasWriteIntent() /* committed value */ ||
		vv.Version == txn.ID.Version() /* read after write */ {
		return vv.Value, nil
	}
	assert.Must(vv.Version < txn.ID.Version())
	var preventFutureWrite = utils.IsTooOld(vv.Version, txn.cfg.WoundUncommittedTxnThreshold)
	writeTxn, err := txn.store.inferTransactionRecordWithRetry(
		ctx,
		types.TxnId(vv.Version), txn,
		map[string]struct{}{key: {}},
		ttypes.KeyVersions{key: consts.PositiveInvalidTxnInternalVersion}, // last internal version not known, use an invalid version instead, don't use 0 because we want to guarantee WriteKeyInfo.NotEmpty()
		preventFutureWrite,
		consts.MaxRetryResolveFoundedWriteIntent)
	if err != nil {
		return types.EmptyValue, errors.Annotatef(errors.ErrReadUncommittedDataPrevTxnStatusUndetermined, "reason: '%v', txn: %d", err, vv.Version)
	}
	assert.Must(writeTxn.ID.Version() == vv.Version)
	committed, rollbackedOrSafeToRollback := writeTxn.checkCommitState(ctx, txn, ttypes.KeyVersions{key: vv.InternalVersion}, preventFutureWrite, consts.MaxRetryResolveFoundedWriteIntent)
	if committed {
		committedTxnInternalVersion := writeTxn.GetCommittedVersion(key)
		assert.Must(committedTxnInternalVersion == 0 || committedTxnInternalVersion == vv.InternalVersion) // no way to write a new version after read
		return vv.Value, nil
	}
	if rollbackedOrSafeToRollback {
		if writeTxn.IsWrittenKeyRollbacked(key) {
			return types.EmptyValue, errors.ErrReadUncommittedDataPrevTxnKeyRollbacked
		}
		assert.Must(writeTxn.State == types.TxnStateRollbacking)
		return types.EmptyValue, errors.Annotatef(errors.ErrReadUncommittedDataPrevTxnToBeRollbacked, "previous txn %d", writeTxn.ID)
	}
	assert.Must(!writeTxn.State.IsTerminated())
	return types.EmptyValue, errors.Annotatef(errors.ErrReadUncommittedDataPrevTxnStatusUndetermined, "previous txn %d", writeTxn.ID)
}

func (txn *Txn) checkCommitState(ctx context.Context, callerTxn *Txn, keysWithWriteIntent ttypes.KeyVersions, preventFutureWrite bool, maxRetry int) (committed bool, rollbackedOrSafeToRollback bool) {
	assert.Must(len(keysWithWriteIntent) > 0)
	switch txn.State {
	case types.TxnStateStaging:
		assert.Must(txn.AreWrittenKeysCompleted())
		for key := range keysWithWriteIntent {
			assert.Must(txn.ContainsWrittenKey(key))
		}
		if txn.GetWrittenKeyCount() == len(keysWithWriteIntent) && txn.MatchWrittenKeys(keysWithWriteIntent) {
			txn.State = types.TxnStateCommitted
			txn.onCommitted(callerTxn.ID, "[CheckCommitState] txn.GetWrittenKeyCount() == len(keysWithWriteIntent) && txn.MatchWrittenKeys(keysWithWriteIntent)") // help commit since original txn coordinator may have gone
			return true, false
		}

		var (
			txnWrittenKeys    = txn.GetWrittenKey2LastVersion()
			inKeys, notInKeys []string
		)
		for writtenKey := range txnWrittenKeys {
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
			vv, exists, err := txn.store.getValueWrittenByTxnWithRetry(ctx, key, txn.ID, callerTxn, preventFutureWrite, true, maxRetry)
			if err != nil {
				return false, false
			}
			if exists {
				assert.Must(vv.Version == txn.ID.Version() && vv.InternalVersion > 0)
				if !vv.HasWriteIntent() {
					txn.State = types.TxnStateCommitted
					txn.MarkCommittedCleared(key, vv.Value)
					txn.onCommitted(callerTxn.ID, "[CheckCommitState] key '%s' write intent cleared", key) // help commit since original txn coordinator may have gone
					return true, false
				}
				if vv.InternalVersion == txnWrittenKeys[key] {
					continue
				}
				assert.Must(vv.InternalVersion < txnWrittenKeys[key])
			}
			if vv.MaxReadVersion > txn.ID.Version() {
				txn.State = types.TxnStateRollbacking
				if !exists {
					txn.MarkWrittenKeyRollbacked(key)
				}
				_ = txn.rollback(ctx, callerTxn.ID, true, "found non existed key '%s' during CheckCommitState"+
					" and is impossible to succeed in the future, max read version(%d) > txnId(%d)", key, vv.MaxReadVersion, txn.ID) // help rollback since original txn coordinator may have gone
				return false, true
			}
			if !exists && idx >= notInKeysLen {
				assert.Must(keysWithWriteIntent.Contains(key))
				txn.State = types.TxnStateRollbacking
				txn.MarkWrittenKeyRollbacked(key)                                                               // key already rollbacked
				_ = txn.rollback(ctx, callerTxn.ID, true, "previous write intent of key '%s' disappeared", key) // help rollback since original txn coordinator may have gone
				return false, true
			}
			return false, false
		}
		txn.State = types.TxnStateCommitted
		txn.onCommitted(callerTxn.ID, "[CheckCommitState] all keys exist and match latest internal txn version") // help commit since original txn coordinator may have gone
		return true, false
	case types.TxnStateCommitted:
		return true, false
	case types.TxnStateRollbacking:
		_ = txn.rollback(ctx, callerTxn.ID, false, "transaction rollbacking, txn error: %v", txn.err) // help rollback since original txn coordinator may have gone
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
		txn.err = errors.Annotatef(errors.ErrWriteReadConflict, "a transaction with higher timestamp has read the key '%s'", key)
		_ = txn.rollback(ctx, txn.ID, true, "error occurred during Txn::Set: %v", txn.err)
		return txn.err
	}

	if err := txn.WriteKey(txn.GenIDOfKey(key), txn.s.ioJobScheduler,
		key, types.NewValue(val, txn.ID.Version()),
		types.NewKVCCWriteOption().CondReadForWrite(txn.Type.IsReadForWrite())); err != nil {
		_ = txn.rollback(ctx, txn.ID, true, "error occurred during Txn::Set: %v", txn.err)
		txn.err = err
		return err
	}
	for _, task := range txn.GetWriteTasks() {
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

	if txn.State != types.TxnStateUncommitted {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s, but got %s", types.TxnStateUncommitted, txn.State)
	}

	if txn.GetWrittenKeyCount() == 0 {
		if txn.Type.IsReadForWrite() && len(txn.readForWriteReadKeys) > 0 { // todo commit instead?
			_ = txn.rollback(ctx, txn.ID, false, "readForWrite transaction write no keys")
			return errors.ErrReadForWriteTransactionCommitWithNoWrittenKeys
		}
		txn.State = types.TxnStateCommitted
		txn.gc()
		return nil
	}

	txn.State = types.TxnStateStaging
	txn.MarkWrittenKeyCompleted()
	recordData := txn.Encode()
	txnRecordTask := types.NewListTaskWithResult(
		txn.txnKey,
		"write-txn-record-during-commit",
		txn.cfg.WoundUncommittedTxnThreshold, func(ctx context.Context, _ interface{}) (interface{}, error) {
			if err := txn.writeTxnRecord(ctx, recordData); err != nil {
				return nil, err
			}
			return basic.DummyTaskResult, nil
		})
	if err := txn.s.ScheduleIOJob(txnRecordTask); err != nil {
		txn.State = types.TxnStateRollbacking
		_ = txn.rollback(ctx, txn.ID, true, "write record io task schedule failed: '%v'", err)
		return err
	}
	txn.txnRecordTask = txnRecordTask

	// Wait finish
	txn.waitIOTasks(ctx, false)

	var lastNonRollbackableIOErr error
	for _, ioTask := range txn.getIOTasks() {
		if ioTaskErr := ioTask.Err(); ioTaskErr != nil {
			if errors.IsMustRollbackCommitErr(ioTaskErr) {
				// write record must failed
				txn.State = types.TxnStateRollbacking
				_ = txn.rollback(ctx, txn.ID, true, "write key %s returns rollbackable error: '%v'", ioTask.ID, ioTaskErr)
				return ioTaskErr
			}
			lastNonRollbackableIOErr = ioTaskErr
		}
	}

	if lastNonRollbackableIOErr == nil {
		// succeeded
		txn.State = types.TxnStateCommitted
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
			txn.State = types.TxnStateInvalid
			return err
		}
		assert.Must(txn.ID == inferredTxn.ID)
		assert.Must(inferredTxn.GetWrittenKeyCount() == txn.GetWrittenKeyCount())
		txn.State = inferredTxn.State
		if txn.State == types.TxnStateCommitted {
			return nil
		}
		if txn.State.IsAborted() {
			return lastNonRollbackableIOErr
		}
	}
	assert.Must(txn.State == types.TxnStateStaging)
	succeededKeys := txn.GetSucceededWrittenKeys()
	assert.Must(len(succeededKeys) != txn.GetWrittenKeyCount() || txn.txnRecordTask.Err() != nil)
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

	return txn.rollback(ctx, txn.ID, true, "rollback by user, txn err: '%v'", txn.err)
}

func (txn *Txn) rollback(ctx context.Context, callerTxn types.TxnId, createTxnRecordOnFailure bool, reason string, args ...interface{}) error { // TODO lazy generate reason?
	if callerTxn != txn.ID && !txn.couldBeWounded() {
		assert.Must(txn.State == types.TxnStateRollbacking)
		return nil
	}
	txn.waitIOTasks(ctx, true)
	if txn.State == types.TxnStateRollbacked {
		return nil
	}

	if txn.State != types.TxnStateUncommitted && txn.State != types.TxnStateRollbacking {
		return errors.Annotatef(errors.ErrTransactionStateCorrupted, "expect: %s or %s, but got %s", types.TxnStateUncommitted, types.TxnStateRollbacking, txn.State)
	}

	assert.Must(txn.txnRecordTask == nil || txn.AreWrittenKeysCompleted())
	assert.Must(txn.State != types.TxnStateStaging || txn.AreWrittenKeysCompleted())
	txn.State = types.TxnStateRollbacking
	var (
		noNeedToRemoveTxnRecord = !txn.AreWrittenKeysCompleted() || (txn.ID == callerTxn && txn.txnRecordTask == nil)
		toClearKeys             = txn.getToClearKeys(true)
	)
	if noNeedToRemoveTxnRecord && len(toClearKeys) == 0 {
		if txn.AreWrittenKeysCompleted() {
			txn.State = types.TxnStateRollbacked
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
			txn.txnKey, "remove-txn-record", txn.cfg.ClearTimeout,
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
		if txn.State == types.TxnStateRollbacking && createTxnRecordOnFailure && !root.ChildrenSuccess(writtenKeyChildren) {
			_ = txn.writeTxnRecord(ctx, txn.Encode()) // make life easier for other transactions // TODO needs to gc later
		}
	}()

	for key, isWrittenKey := range toClearKeys {
		var (
			key          = key
			isWrittenKey = isWrittenKey
			val          = types.NewValue(nil, txn.ID.Version()).WithNoWriteIntent()
			opt          = types.NewKVCCWriteOption().WithRollbackVersion().
					CondReadForWrite(txn.Type.IsReadForWrite()).CondReadForWriteRollbackOrClearReadKey(!isWrittenKey).
					CondWriteByDifferentTransaction(callerTxn != txn.ID)
		)
		if child := types.NewTreeTaskNoResult(
			txn.GenIDOfKey(key), "rollback-key", txn.cfg.ClearTimeout, root, func(ctx context.Context, _ []interface{}) error {
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
			txn.MarkWrittenKeyRollbacked(txn.RetrieveKeyFromID(c.ID))
		}
	}
	if err := root.Err(); err != nil {
		return err
	}

	if txn.AreWrittenKeysCompleted() {
		txn.State = types.TxnStateRollbacked
		if callerTxn == txn.ID {
			txn.gc()
		}
	}
	return nil
}

func (txn *Txn) onCommitted(callerTxn types.TxnId, reason string, args ...interface{}) {
	assert.Must(txn.State == types.TxnStateCommitted && txn.AreWrittenKeysCompleted())
	assert.Must(txn.GetWrittenKeyCount() > 0)

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

	var root = types.NewTreeTaskNoResult(
		txn.txnKey, "remove-txn-record", txn.cfg.ClearTimeout,
		nil, func(ctx context.Context, _ []interface{}) error {
			if consts.BuildOption.IsDebug() { // TODO remove this in product
				txn.ForEachWrittenKey(func(key string, _ ttypes.WriteKeyInfo) {
					val, exists, err := txn.store.getValueWrittenByTxnWithRetry(ctx, key, txn.ID, txn, false, false, 1)
					assert.Must(err == nil && (exists && !val.HasWriteIntent()))
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
					CondReadForWrite(txn.Type.IsReadForWrite()).CondReadForWriteRollbackOrClearReadKey(!isWrittenKey).
					CondWriteByDifferentTransaction(callerTxn != txn.ID)
		)
		_ = types.NewTreeTaskNoResult(
			txn.GenIDOfKey(key), "clear-key", txn.cfg.ClearTimeout, root, func(ctx context.Context, _ []interface{}) error {
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
	if txn.Type.IsReadForWrite() {
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

func (txn *Txn) waitIOTasks(ctx context.Context, forceCancel bool) {
	if txn.allIOTasksFinished {
		return
	}

	var (
		ioTasks        = txn.getIOTasks()
		cancelledTasks = make([]*types.ListTask, 0, len(ioTasks))
	)
	if len(ioTasks) == 0 {
		return
	}
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
	for _, ioTask := range ioTasks {
		assert.Must(ioTask.Finished()) // TODO remove this in product
	}
	txn.s.GCIOJobs(ioTasks)
	txn.allIOTasksFinished = true
}

func (txn *Txn) getIOTasks() []*types.ListTask {
	ioTasks := txn.GetCopiedWriteTasks()
	if txn.txnRecordTask != nil {
		ioTasks = append(ioTasks, txn.txnRecordTask)
	}
	return ioTasks
}

func (txn *Txn) writeTxnRecord(ctx context.Context, recordData []byte) error {
	// set write intent so that other transactions can stop this txn from committing,
	// thus implement the safe-rollback functionality
	err := txn.kv.Set(ctx, txn.txnKey, types.NewValue(recordData, txn.ID.Version()), types.NewKVCCWriteOption().WithTxnRecord())
	if err != nil {
		glog.V(7).Infof("[writeTxnRecord] write transaction record failed: %v", err)
	}
	return err
}

func (txn *Txn) removeTxnRecord(ctx context.Context, rollback bool) error {
	if err := txn.kv.Set(ctx, txn.txnKey,
		types.NewValue(nil, txn.ID.Version()).WithNoWriteIntent(),
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
