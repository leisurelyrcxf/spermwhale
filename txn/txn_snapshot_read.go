package txn

import (
	"context"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/basic"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

func (txn *Txn) getSnapshot(ctx context.Context, key string) (types.TValue, error) {
	txnSnapshotVersion := txn.SnapshotVersion
	if txnSnapshotVersion == 0 {
		txnSnapshotVersion = txn.ID.Version()
	}
	val, err := txn.kv.Get(ctx, key, txn.GetSnapshotKVCCReadOption(txnSnapshotVersion))
	if err != nil {
		if errors.IsSnapshotReadTabletErr(err) {
			txn.assertSnapshotReadResult(val, txnSnapshotVersion, true)
			txn.SetSnapshotVersion(val.Version-1, false)
		}
		return types.EmptyTValue, err
	}
	txn.assertSnapshotReadResult(val, txnSnapshotVersion, false)
	txn.SetSnapshotVersion(val.SnapshotVersion, false)
	txn.MinAllowedSnapshotVersion = utils.MaxUint64(txn.MinAllowedSnapshotVersion, val.Version)
	txn.assertSnapshot()
	return val.ToTValue(), nil
}

func (txn *Txn) mgetSnapshot(ctx context.Context, keys []string) (_ []types.TValue, err error) {
	if len(keys) == 1 {
		val, err := txn.getSnapshot(ctx, keys[0])
		if err != nil {
			return nil, err
		}
		return []types.TValue{val}, nil
	}

	var (
		readKeys, readResult = basic.MakeSet(keys), make(types.ReadResultCC)
		snapshotReadOpt      = txn.TxnSnapshotReadOption.WithClearDontAllowsVersionBack()
	)
	if snapshotReadOpt.SnapshotVersion == 0 {
		snapshotReadOpt.SnapshotVersion = txn.ID.Version()
	}
	for ctx.Err() == nil {
		var tasks = make([]*basic.Task, 0, len(readKeys))
		if len(readKeys) == 1 {
			readKey := readKeys.MustFirst()
			tasks = append(tasks, basic.NewTask(basic.NewTaskId(txn.ID.Version(), readKey), "get", consts.DefaultReadTimeout, func(ctx context.Context) (i interface{}, err error) {
				return txn.kv.Get(ctx, readKey, txn.GetSnapshotKVCCReadOption(snapshotReadOpt.SnapshotVersion))
			}))
			_ = tasks[0].Run()
		} else {
			for readKey := range readKeys {
				var (
					readKey = readKey
					readOpt = txn.GetSnapshotKVCCReadOption(snapshotReadOpt.SnapshotVersion)
				)
				task := basic.NewTask(basic.NewTaskId(txn.ID.Version(), readKey), "get", consts.DefaultReadTimeout, func(ctx context.Context) (i interface{}, err error) {
					return txn.kv.Get(ctx, readKey, readOpt)
				})
				if err := txn.s.ScheduleReadJob(task); err != nil {
					for _, t := range tasks {
						t.Cancel()
					}
					return nil, err
				}
				tasks = append(tasks, task)
			}
			for i, task := range tasks {
				if !task.WaitFinishWithContext(ctx) {
					for _, t := range tasks[i:] {
						t.Cancel()
					}
					assert.Must(ctx.Err() != nil)
					return nil, ctx.Err()
				}
			}
		}
		var lastTabletErr, lastErr error
		oldSnapshotVersion := snapshotReadOpt.SnapshotVersion
		// Check error and update snapshotReadOpt.SnapshotVersion
		for _, task := range tasks {
			if val, err := task.Result().(types.ValueCC), task.Err(); err != nil {
				if errors.IsSnapshotReadTabletErr(err) {
					txn.assertSnapshotReadResult(val, oldSnapshotVersion, true)
					snapshotReadOpt.SetSnapshotVersion(val.Version-1, false)
					lastTabletErr = err
				}
				lastErr = err
			} else {
				txn.assertSnapshotReadResult(val, oldSnapshotVersion, false)
				readResult[task.ID.Key] = val
				snapshotReadOpt.SetSnapshotVersion(val.SnapshotVersion, false)
			}
		}
		if lastTabletErr != nil { // return tablets error preferentially
			txn.SetSnapshotVersion(snapshotReadOpt.SnapshotVersion, false)
			return nil, lastTabletErr
		}
		if lastErr != nil {
			return nil, lastErr
		}

		assert.Must(snapshotReadOpt.SnapshotVersion > 0)
		// All tasks succeeded, check values' version against snapshotReadOpt.SnapshotVersion
		// NOTE this will check version of read values of previous rounds too, otherwise is unsafe.
		readKeys.Reset()
		for key, value := range readResult {
			if value.Version > snapshotReadOpt.SnapshotVersion {
				readKeys.InsertUnsafe(key) // value.version violates snapshotReadOpt.SnapshotVersion
			}
		}
		if len(readKeys) == 0 {
			for _, value := range readResult {
				txn.MinAllowedSnapshotVersion = utils.MaxUint64(txn.MinAllowedSnapshotVersion, value.Version)
			}
			txn.SetSnapshotVersion(snapshotReadOpt.SnapshotVersion, false)
			assert.Must(txn.SnapshotVersion == snapshotReadOpt.SnapshotVersion)
			txn.assertSnapshot()
			return readResult.ToTValues(keys, txn.SnapshotVersion), nil
		}
	}
	return nil, ctx.Err()
}

func (txn *Txn) BeginSnapshotReadTxn(opt types.TxnSnapshotReadOption) error {
	if opt.IsExplicitSnapshotVersion() {
		if !opt.IsRelativeSnapshotVersion() {
			if opt.SnapshotVersion == 0 {
				return errors.Annotatef(errors.ErrInvalidTxnSnapshotReadOption, "!opt.IsRelativeSnapshotVersion() && opt.SnapshotVersion == 0")
			}
			if opt.SnapshotVersion > txn.ID.Version() {
				return errors.Annotatef(errors.ErrInvalidTxnSnapshotReadOption, "!opt.IsRelativeSnapshotVersion() && opt.SnapshotVersion > txn.ID.Version()")
			}
		} else if txn.ID.Version() < opt.SnapshotVersion {
			return errors.Annotatef(errors.ErrInvalidTxnSnapshotReadOption, "opt.IsRelativeSnapshotVersion() && txn.ID.Version() < opt.SnapshotVersion ")
		}
	}
	if opt.IsRelativeMinAllowedSnapshotVersion() && txn.ID.Version() < opt.MinAllowedSnapshotVersion {
		return errors.Annotatef(errors.ErrInvalidTxnSnapshotReadOption, "opt.IsRelativeMinAllowedSnapshotVersion() && txn.ID.Version() < opt.MinAllowedSnapshotVersion ")
	}
	if (opt.SnapshotVersion != 0 && opt.SnapshotVersion < opt.MinAllowedSnapshotVersion) ||
		(opt.SnapshotVersion == 0 && txn.ID.Version() < opt.MinAllowedSnapshotVersion) {
		return errors.Annotatef(errors.ErrInvalidTxnSnapshotReadOption, errors.ErrMinAllowedSnapshotVersionViolated.Msg)
	}

	txn.TxnSnapshotReadOption = opt
	if opt.IsRelativeMinAllowedSnapshotVersion() {
		txn.MinAllowedSnapshotVersion = txn.ID.Version() - opt.MinAllowedSnapshotVersion
	}
	if opt.IsExplicitSnapshotVersion() && opt.IsRelativeSnapshotVersion() {
		txn.SnapshotVersion = txn.ID.Version() - opt.SnapshotVersion
	}
	if !txn.TxnSnapshotReadOption.IsEmpty() && bool(glog.V(60)) {
		glog.Infof("[Txn::BeginSnapshotReadTxn] txn-%d snapshot read option initialized to %s", txn.ID, txn.TxnSnapshotReadOption)
	}
	return nil
}

func (txn *Txn) GetSnapshotKVCCReadOption(snapshotVersion uint64) types.KVCCReadOption {
	assert.Must(snapshotVersion != 0)
	minAllowedSnapshotVersion := txn.MinAllowedSnapshotVersion
	if !txn.AllowsVersionBack() {
		minAllowedSnapshotVersion = utils.MaxUint64(minAllowedSnapshotVersion, txn.SnapshotVersion)
	}
	return types.NewSnapshotKVCCReadOption(snapshotVersion, minAllowedSnapshotVersion).CondWaitWhenReadDirty(txn.IsWaitWhenReadDirty())
}

func (txn *Txn) assertSnapshotReadResult(val types.ValueCC, readSnapshotVersion uint64, isDirty bool) {
	if isDirty {
		assert.Must(val.IsDirty() && val.V == nil)
	} else {
		assert.Must(!val.IsDirty() && val.V != nil)
	}
	assert.Must(val.SnapshotVersion > 0 && val.Version <= val.SnapshotVersion &&
		txn.MinAllowedSnapshotVersion <= val.SnapshotVersion && val.SnapshotVersion <= readSnapshotVersion)
}

func (txn *Txn) assertSnapshot() {
	if txn.SnapshotVersion < txn.MinAllowedSnapshotVersion {
		panic("expect txn.SnapshotVersion >= txn.minAllowedSnapshotVersion, but got false")
	}
}
