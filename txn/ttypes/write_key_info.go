package ttypes

import (
	"context"
	"time"

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
	LastWrittenVersion types.TxnInternalVersion
	LastTask           *types.ListTask

	CommittedCleared, Committed, Rollbacked bool
}

func NewWriteKeyInfo(lastWrittenVersion types.TxnInternalVersion) WriteKeyInfo {
	assert.Must(lastWrittenVersion > 0)
	return WriteKeyInfo{
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
	assert.Must(ks.keys == nil)
	ks.keys = make(map[string]WriteKeyInfo)
	for key, lastWrittenVersion := range key2LastWrittenVersion {
		ks.keys[key] = NewWriteKeyInfo(lastWrittenVersion)
	}
	ks.completed = completed
}

func (ks WriteKeyInfos) GetWriteTasks() []*types.ListTask {
	return ks.tasks
}

func (ks WriteKeyInfos) GetCopiedWriteTasks() []*types.ListTask {
	return append(make([]*types.ListTask, 0, len(ks.tasks)), ks.tasks...)
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

func (ks WriteKeyInfos) GetSucceededWrittenKeys() KeyVersions {
	assert.Must(ks.completed)

	succeededKeys := make(KeyVersions)
	for key, v := range ks.keys {
		if v.LastTask.Err() == nil {
			assert.Must(v.LastTask.Data.(types.TxnInternalVersion) == v.LastWrittenVersion && v.LastWrittenVersion > 0)
			succeededKeys[key] = v.LastWrittenVersion
		}
	}
	return succeededKeys
}

func (ks WriteKeyInfos) GetWrittenKeyCount() int {
	return len(ks.keys)
}

func (ks WriteKeyInfos) GetCommittedVersion(key string) types.TxnInternalVersion {
	v, ok := ks.keys[key]
	assert.Must(ok && v.Committed) // not sure key may not exist
	assert.Must(v.LastTask == nil || v.LastTask.Data.(types.TxnInternalVersion) == v.LastWrittenVersion)

	return v.LastWrittenVersion
}

func (ks WriteKeyInfos) AreWrittenKeysCompleted() bool {
	return ks.completed
}

func (ks WriteKeyInfos) ContainsWrittenKey(key string) bool {
	_, ok := ks.keys[key]
	return ok
}

func (ks WriteKeyInfos) IsWrittenKeyRollbacked(key string) bool {
	v := ks.keys[key]
	assert.Must(!v.Committed)
	return v.Rollbacked
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
		assert.Must(!v.Rollbacked)
		v.Committed = true
		ks.keys[k] = v
	}
}

func (ks WriteKeyInfos) MarkWrittenKeyRollbacked(key string) {
	v, ok := ks.keys[key]
	assert.Must(ok && !v.Committed && !v.CommittedCleared)
	v.Rollbacked = true

	ks.keys[key] = v
}

func (ks WriteKeyInfos) MarkCommittedCleared(key string, value types.Value) {
	assert.Must(!value.HasWriteIntent())
	//v, ok := ks.keys[key]
	//assert.Must(ok) // key may not exist
	//assert.Must(v.LastWrittenVersion == consts. == value.InternalVersion)
	//assert.Must(v.LastTask == nil || v.LastTask.Data.(uint8) == value.InternalVersion)

	v := ks.keys[key]
	assert.Must(!ks.completed || v.LastWrittenVersion == value.InternalVersion)
	v.LastWrittenVersion = value.InternalVersion
	v.Committed = true
	v.CommittedCleared = true

	ks.keys[key] = v
}

// Below are write methods, need pass pointers

func (ks *WriteKeyInfos) WriteKey(taskId string, s *scheduler.ConcurrentDynamicListScheduler, key string, val types.Value, opt types.KVCCWriteOption) error {
	assert.Must(!ks.completed)
	var v = WriteKeyInfo{LastWrittenVersion: 0}
	if ks.keys == nil {
		ks.keys = map[string]WriteKeyInfo{}
	} else {
		v = ks.keys[key]
	}
	v.LastWrittenVersion++
	val = val.WithInternalVersion(v.LastWrittenVersion)
	task := types.NewListTaskWithResult(taskId, "set", ks.TaskTimeout, func(ctx context.Context, prevResult interface{}) (i interface{}, err error) {
		if err := ks.KV.Set(ctx, key, val, opt); err != nil {
			return nil, err
		}
		return basic.DummyTaskResult, nil
	})
	if err := s.Schedule(task); err != nil { // TODO add context for schedule
		return errors.Annotatef(err, "schedule write task of key '%s' failed", key)
	}
	task.Data = val.InternalVersion
	v.LastTask = task

	ks.keys[key] = v
	ks.tasks = append(ks.tasks, task)
	return nil
}

// MarkWrittenKeyCompleted, keys and LastWrittenVersion won't change after this call.
func (ks *WriteKeyInfos) MarkWrittenKeyCompleted() {
	ks.completed = true
}
