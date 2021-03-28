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

func (txn *Txn) getSnapshot(ctx context.Context, key string) (types.Value, error) {
	val, err := txn.kv.Get(ctx, key, txn.GetSnapshotKVCCReadOption())
	if err != nil {
		if errors.IsSnapshotReadTabletErr(err) {
			txn.assertSnapshotReadResult(val, txn.SnapshotVersion, true)
			txn.SetSnapshotVersion(val.Version - 1)
		}
		return types.EmptyValue, err
	}
	txn.assertSnapshotReadResult(val, txn.SnapshotVersion, false)
	txn.SetSnapshotVersion(val.SnapshotVersion)
	txn.minAllowedSnapshotVersion = utils.MaxUint64(txn.minAllowedSnapshotVersion, val.Version)
	txn.assertSnapshot()
	return val.Value, nil
}

func (txn *Txn) mgetSnapshot(ctx context.Context, keys []string) (_ []types.Value, err error) {
	if len(keys) == 1 {
		val, err := txn.getSnapshot(ctx, keys[0])
		if err != nil {
			return nil, err
		}
		return []types.Value{val}, nil
	}

	var readKeys, readResult = basic.MakeSet(keys), make(types.ReadResultCC)
	for ctx.Err() == nil {
		var tasks = make([]*basic.Task, 0, len(readKeys))
		if len(readKeys) == 1 {
			readKey := readKeys.MustFirst()
			tasks = append(tasks, basic.NewTask(basic.NewTaskId(txn.ID.Version(), readKey), "get", consts.DefaultReadTimeout, func(ctx context.Context) (i interface{}, err error) {
				return txn.kv.Get(ctx, readKey, txn.GetSnapshotKVCCReadOption())
			}))
			_ = tasks[0].Run()
		} else {
			for readKey := range readKeys {
				var (
					readKey = readKey
					readOpt = txn.GetSnapshotKVCCReadOption()
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
		oldSnapshotVersion := txn.SnapshotVersion
		// Check error and update txn.SnapshotVersion
		for _, task := range tasks {
			if val, err := task.Result().(types.ValueCC), task.Err(); err != nil {
				if errors.IsSnapshotReadTabletErr(err) {
					txn.assertSnapshotReadResult(val, oldSnapshotVersion, true)
					txn.SetSnapshotVersion(val.Version - 1)
					lastTabletErr = err
				}
				lastErr = err
			} else {
				txn.assertSnapshotReadResult(val, oldSnapshotVersion, false)
				readResult[task.ID.Key] = val
				txn.SetSnapshotVersion(val.SnapshotVersion)
			}
		}
		if lastTabletErr != nil { // return tablets error preferentially
			return nil, lastTabletErr
		}
		if lastErr != nil {
			return nil, lastErr
		}

		// All tasks succeeded, check values' version against txn.SnapshotVersion
		// NOTE this will check version of read values of previous rounds too, otherwise is unsafe.
		readKeys.Reset()
		for key, value := range readResult {
			if value.Version > txn.SnapshotVersion {
				readKeys.InsertUnsafe(key) // value.version violates txn.SnapshotVersion
			}
		}
		if len(readKeys) == 0 {
			for _, value := range readResult {
				txn.minAllowedSnapshotVersion = utils.MaxUint64(txn.minAllowedSnapshotVersion, value.Version)
			}
			txn.assertSnapshot()
			return readResult.ToValues(keys, txn.SnapshotVersion), nil
		}
	}
	return nil, ctx.Err()
}

func (txn *Txn) SetSnapshotReadOption(opt types.TxnSnapshotReadOption) error {
	if opt.IsExplicitSnapshotVersion() {
		if opt.SnapshotVersion == 0 {
			return errors.Annotatef(errors.ErrInvalidExplicitSnapshotVersion, "opt.SnapshotVersion == 0")
		}
		if opt.SnapshotVersion > txn.ID.Version() {
			return errors.Annotatef(errors.ErrInvalidExplicitSnapshotVersion, "opt.SnapshotVersion > txn.ID.Version()")
		}
	}
	txn.TxnSnapshotReadOption = opt
	if !txn.TxnSnapshotReadOption.IsEmpty() && bool(glog.V(60)) {
		glog.Infof("[Txn::SetSnapshotReadOption] txn-%d snapshot read option initialized to %s", txn.ID, txn.TxnSnapshotReadOption)
	}

	return nil
}

func (txn *Txn) GetSnapshotKVCCReadOption() types.KVCCReadOption {
	var (
		snapshotVersion, minAllowedSnapshotVersion uint64
	)
	if snapshotVersion = txn.SnapshotVersion; snapshotVersion == 0 {
		snapshotVersion = txn.ID.Version()
	}
	if minAllowedSnapshotVersion = txn.minAllowedSnapshotVersion; !txn.AllowsVersionBack() {
		minAllowedSnapshotVersion = snapshotVersion
	}
	return types.NewKVCCReadOption(0).WithSnapshotRead(snapshotVersion, minAllowedSnapshotVersion).
		CondWaitWhenReadDirty(txn.IsWaitWhenReadDirty())
}

func (txn *Txn) assertSnapshotReadResult(val types.ValueCC, readSnapshotVersion uint64, isDirty bool) {
	if isDirty {
		assert.Must(val.IsDirty() && val.V == nil)
	} else {
		assert.Must(!val.IsDirty() && val.V != nil)
	}
	assert.Must(val.SnapshotVersion > 0 && val.Version <= val.SnapshotVersion &&
		txn.minAllowedSnapshotVersion <= val.SnapshotVersion && val.SnapshotVersion <= readSnapshotVersion)
}

func (txn *Txn) assertSnapshot() {
	if txn.SnapshotVersion < txn.minAllowedSnapshotVersion {
		panic("expect txn.SnapshotVersion >= txn.minAllowedSnapshotVersion, but got false")
	}
}
