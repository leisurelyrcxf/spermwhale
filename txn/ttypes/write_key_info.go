package ttypes

import (
	"context"
	"time"

	"github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/scheduler"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/basic"
)

var EmptyWriteKeyInfo = WriteKeyInfo{}

type KeyVersions map[string]types.TxnInternalVersion

func (kvs KeyVersions) Contains(key string) bool {
	_, ok := kvs[key]
	return ok
}

func (kvs KeyVersions) MustFirstKey() string {
	for key := range kvs {
		return key
	}
	panic("KeyVersions empty")
}

type WriteKeyInfo struct {
	types.KeyState

	LastWrittenVersion types.TxnInternalVersion
	LastTask           *types.ListTask
}

func NewWriteKeyInfo(lastWrittenVersion types.TxnInternalVersion) WriteKeyInfo {
	assert.Must(lastWrittenVersion > 0)
	return WriteKeyInfo{
		KeyState:           types.KeyStateUncommitted,
		LastWrittenVersion: lastWrittenVersion,
	}
}

func (ks WriteKeyInfo) IsEmpty() bool {
	return ks.LastWrittenVersion == 0
}

type WriteKeyInfos struct {
	keys  map[string]WriteKeyInfo
	tasks []*types.ListTask

	KV          types.KVCC
	TaskTimeout time.Duration
	completed   bool
}

func (ks *WriteKeyInfos) InitializeWrittenKeys(key2LastWrittenVersion KeyVersions, completed bool) {
	assert.Must(ks.keys == nil && len(key2LastWrittenVersion) > 0)
	ks.keys = make(map[string]WriteKeyInfo)
	for key, lastWrittenVersion := range key2LastWrittenVersion {
		ks.keys[key] = NewWriteKeyInfo(lastWrittenVersion)
	}
	ks.completed = completed
}

func (ks WriteKeyInfos) GetWriteKeyTasks() []*types.ListTask {
	return ks.tasks
}

func (ks WriteKeyInfos) GetCopiedWriteKeyTasks() []*basic.Task {
	tasks := make([]*basic.Task, 0, len(ks.tasks)+1)
	for _, t := range ks.tasks {
		tasks = append(tasks, &t.Task)
	}
	return tasks
}

func (ks WriteKeyInfos) GetCopiedWriteKeyTasksEx() ([]*basic.Task, []*types.ListTask) {
	tasks := make([]*basic.Task, 0, len(ks.tasks)+1) // for append
	listTasks := make([]*types.ListTask, 0, len(ks.tasks))
	for _, t := range ks.tasks {
		tasks = append(tasks, &t.Task)
		listTasks = append(listTasks, t)
	}
	return tasks, listTasks
}

func (ks WriteKeyInfos) GetLastWriteKeyTasks(allWriteKeyTasks []*types.ListTask) []*types.ListTask {
	if len(ks.keys) == len(allWriteKeyTasks) {
		return allWriteKeyTasks
	}
	// Has write after write
	tasks := make([]*types.ListTask, 0, len(ks.keys))
	for _, v := range ks.keys {
		if v.LastTask != nil {
			tasks = append(tasks, v.LastTask)
		}
	}
	return tasks
}

func (ks WriteKeyInfos) ForEachWrittenKey(f func(key string, info WriteKeyInfo)) {
	for k, v := range ks.keys {
		f(k, v)
	}
}

func (ks WriteKeyInfos) GetWrittenKey2LastVersion() (keys KeyVersions) {
	keys = make(KeyVersions)
	for k, v := range ks.keys {
		assert.Must(v.LastWrittenVersion.IsValid())
		keys[k] = v.LastWrittenVersion
	}
	return keys
}

func (ks WriteKeyInfos) GetLastWriteKeyTask(key string) *types.ListTask {
	return ks.keys[key].LastTask
}

func (ks WriteKeyInfos) GetSucceededWrittenKeysUnsafe() map[string]types.ValueCC {
	assert.Must(ks.completed)

	succeededKeys := make(map[string]types.ValueCC)
	for key, v := range ks.keys {
		if v.LastTask.Err() == nil {
			assert.Must(v.LastTask.Data.(types.TxnInternalVersion) == v.LastWrittenVersion && v.LastWrittenVersion > 0)
			succeededKeys[key] = types.ValueCC{
				Value: types.Value{
					Meta: types.Meta{
						InternalVersion: v.LastWrittenVersion, // actually only the txn internal version will be used
					},
				},
			}
		}
	}
	return succeededKeys
}

func (ks WriteKeyInfos) GetWrittenKeyCount() int {
	return len(ks.keys)
}

func (ks WriteKeyInfos) GetCommittedVersion(key string) types.TxnInternalVersion {
	v, ok := ks.keys[key]
	assert.Must(ok && v.IsCommitted()) // not sure key may not exist
	assert.Must(v.LastTask == nil || v.LastTask.Data.(types.TxnInternalVersion) == v.LastWrittenVersion)

	return v.LastWrittenVersion
}

func (ks WriteKeyInfos) MustGetInternalVersion(key string) types.TxnInternalVersion {
	v, ok := ks.keys[key]
	assert.Must(ok)
	return v.LastWrittenVersion
}

func (ks WriteKeyInfos) MustGetKeyState(key string) types.KeyState {
	v, ok := ks.keys[key]
	assert.Must(ok)
	return v.KeyState
}

func (ks WriteKeyInfos) GetKeyStateUnsafe(key string) types.KeyState {
	return ks.keys[key].KeyState
}

func (ks WriteKeyInfos) AreWrittenKeysCompleted() bool {
	return ks.completed
}

func (ks WriteKeyInfos) ContainsWrittenKey(key string) bool {
	_, ok := ks.keys[key]
	return ok
}

func (ks WriteKeyInfos) MatchWrittenKeys(keysWithWriteIntent KeyVersions) bool {
	for k, v := range keysWithWriteIntent {
		lastWrittenVersion := ks.keys[k].LastWrittenVersion
		assert.Must(lastWrittenVersion.IsValid())
		if lastWrittenVersion != v {
			return false
		}
	}
	return true
}

// Update key info methods, no need to pass pointer

func (ks WriteKeyInfos) MarkAllWrittenKeysCommitted() {
	for k, v := range ks.keys {
		v.SetCommitted()
		ks.keys[k] = v
	}
}

func (ks WriteKeyInfos) MarkWrittenKeyCommitted(key string, val types.Value) {
	v := ks.keys[key]
	assert.Must(!ks.completed || v.LastWrittenVersion == val.InternalVersion)
	v.LastWrittenVersion = val.InternalVersion
	v.KeyState = val.GetKeyState()
	ks.keys[key] = v
}

func (ks WriteKeyInfos) MarkWrittenKeyAborted(key string, notExistsErrSubCode int32) {
	v := ks.keys[key]
	if notExistsErrSubCode == consts.ErrSubCodeKeyOrVersionNotExistsInDB {
		v.KeyState = types.KeyStateRollbackedCleared
	} else if notExistsErrSubCode == consts.ErrSubCodeKeyOrVersionNotExistsExistsInDBButRollbacking {
		v.KeyState = types.KeyStateRollbacking
	} else {
		panic(errors.UnreachableCode)
	}
	ks.keys[key] = v
}

func (ks WriteKeyInfos) MarkWrittenKeyRollbackedCleared(key string) {
	v := ks.keys[key]
	v.KeyState = types.KeyStateRollbackedCleared
	ks.keys[key] = v
}

// MarkWrittenKeysCompleted, keys and LastWrittenVersion won't change after this call.
func (ks *WriteKeyInfos) MarkWrittenKeysCompleted() {
	ks.completed = true
}

// Below are write methods, need pass pointers

func (ks *WriteKeyInfos) WriteKey(s *scheduler.ConcurrentDynamicListScheduler, key string, val types.Value, opt types.KVCCWriteOption) error {
	assert.Must(!ks.completed)
	var v = WriteKeyInfo{LastWrittenVersion: 0}
	if ks.keys == nil {
		ks.keys = map[string]WriteKeyInfo{}
	} else {
		v = ks.keys[key]
	}
	if v.LastWrittenVersion+1 > types.TxnInternalVersionMax {
		return errors.ErrTransactionInternalVersionOverflow
	}
	v.LastWrittenVersion++
	val = val.WithInternalVersion(v.LastWrittenVersion)
	task := types.NewListTaskWithResult(basic.NewTaskId(val.Version, key), "write-key", ks.TaskTimeout, func(ctx context.Context, prevResult interface{}) (i interface{}, err error) {
		if err := ks.KV.Set(ctx, key, val, opt); err != nil {
			return nil, err
		}
		return basic.DummyTaskResult, nil
	})
	if err := s.ScheduleListTask(task); err != nil { // TODO add context for schedule
		return errors.Annotatef(err, "schedule write task of key '%s' failed", key)
	}
	task.Data = val.InternalVersion
	v.LastTask = task

	ks.keys[key] = v
	ks.tasks = append(ks.tasks, task)
	return nil
}

// only sued for test

func (ks WriteKeyInfos) setLastTask(key string, t *types.ListTask) {
	v := ks.keys[key]
	v.LastTask = t
	ks.keys[key] = v
}
