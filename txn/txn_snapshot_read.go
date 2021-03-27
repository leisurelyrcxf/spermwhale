package txn

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/basic"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

func (txn *Txn) getSnapshot(ctx context.Context, key string) (types.Value, error) {
	val, err := txn.kv.Get(ctx, key, txn.GetSnapshotReadOption())
	if err != nil {
		if errors.IsSnapshotReadTabletErr(err) {
			txn.assertSnapshotReadResult(val, txn.SnapshotVersion, true)
			txn.SnapshotVersion = val.Version - 1
		}
		return types.EmptyValue, err
	}
	txn.assertSnapshotReadResult(val, txn.SnapshotVersion, false)
	txn.SnapshotVersion = val.SnapshotVersion
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
				return txn.kv.Get(ctx, readKey, txn.GetSnapshotReadOption())
			}))
			_ = tasks[0].Run()
		} else {
			for readKey := range readKeys {
				var (
					readKey = readKey
					readOpt = txn.GetSnapshotReadOption()
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
					txn.SnapshotVersion = utils.MinUint64(txn.SnapshotVersion, val.Version-1)
					lastTabletErr = err
				}
				lastErr = err
			} else {
				txn.assertSnapshotReadResult(val, oldSnapshotVersion, false)
				readResult[task.ID.Key] = val
				txn.SnapshotVersion = utils.MinUint64(txn.SnapshotVersion, val.SnapshotVersion)
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

func (txn *Txn) SetSnapshotVersion(v uint64) {
	assert.Must(v != 0)
	txn.SnapshotVersion = v
	//txn.minAllowedSnapshotVersion = v // explicit version, can't down version
}

func (txn *Txn) GetSnapshotReadOption() types.KVCCReadOption {
	return types.NewKVCCReadOption(0).WithSnapshotRead(txn.SnapshotVersion, txn.minAllowedSnapshotVersion).
		CondWaitWhenReadDirty(txn.IsWaitWhenReadDirty())
}

func (txn *Txn) assertSnapshotReadResult(val types.ValueCC, readSnapshotVersion uint64, expHasWriteIntent bool) {
	if expHasWriteIntent {
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
