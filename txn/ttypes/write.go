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

type WriteKeyInfo struct {
	ValidFlag       uint8
	InternalVersion uint8
	LastTask        *types.ListTask

	CommittedCleared, Committed, Rollbacked bool
}

func NewWriteKeyInfo(internalVersion uint8) WriteKeyInfo {
	return WriteKeyInfo{
		ValidFlag:       1,
		InternalVersion: internalVersion,
	}
}

func (ks WriteKeyInfo) IsValid() bool {
	return ks.ValidFlag > 0
}

type WriteKeyInfos struct {
	keys  map[string]WriteKeyInfo
	tasks []*types.ListTask

	KV          types.KVCC
	TaskTimeout time.Duration
}

func (ks *WriteKeyInfos) InitializeWrittenKeyInfos(keysWithVersions map[string]uint8) {
	assert.Must(ks.keys == nil)
	ks.keys = make(map[string]WriteKeyInfo)
	for key, internalVersion := range keysWithVersions {
		ks.keys[key] = NewWriteKeyInfo(internalVersion)
	}
}

func (ks *WriteKeyInfos) InitializeWrittenKeyInfosNaive(keys basic.Set) {
	assert.Must(ks.keys == nil)
	ks.keys = make(map[string]WriteKeyInfo)
	for key := range keys {
		ks.keys[key] = NewWriteKeyInfo(0)
	}
}

func (ks WriteKeyInfos) GetWriteTasks() []*types.ListTask {
	return ks.tasks
}

func (ks WriteKeyInfos) GetCopiedWriteTasks() []*types.ListTask {
	return append(make([]*types.ListTask, 0, len(ks.tasks)), ks.tasks...)
}

func (ks WriteKeyInfos) GetWrittenKeys() (keys basic.Set) {
	keys = make(basic.Set)
	for k := range ks.keys {
		keys[k] = struct{}{}
	}
	return keys
}

func (ks WriteKeyInfos) ForEachWrittenKey(f func(key string, info WriteKeyInfo)) {
	for k, v := range ks.keys {
		f(k, v)
	}
}

func (ks WriteKeyInfos) GetWrittenKeysWithLastInternalVersion() (keys map[string]uint8) {
	keys = make(map[string]uint8)
	for k, v := range ks.keys {
		keys[k] = v.InternalVersion
	}
	return keys
}

func (ks WriteKeyInfos) GetLastWriteKeyTask(key string) *types.ListTask {
	return ks.keys[key].LastTask
}

func (ks WriteKeyInfos) GetSucceededWrittenKeys() map[string]uint8 {
	succeededKeys := make(map[string]uint8)
	for key, v := range ks.keys {
		if v.LastTask.Err() == nil {
			assert.Must(v.LastTask.Data.(uint8) == v.InternalVersion)
			succeededKeys[key] = v.InternalVersion
		}
	}
	return succeededKeys
}

func (ks WriteKeyInfos) GetWrittenKeyCount() int {
	return len(ks.keys)
}

func (ks WriteKeyInfos) GetCommittedValueInternalVersion(key string) uint8 {
	v, ok := ks.keys[key]
	assert.Must(ok)
	assert.Must(v.Committed)
	assert.Must(v.LastTask == nil || v.LastTask.Data.(uint8) == v.InternalVersion)

	return v.InternalVersion
}

func (ks WriteKeyInfos) GetFinishedKeys(rollback bool) map[string]bool /* key->rollbacked */ {
	m := make(map[string]bool)
	for k, v := range ks.keys {
		assert.Must((rollback && !v.Committed && !v.CommittedCleared) || (!rollback && !v.Rollbacked))
		if v.Rollbacked {
			m[k] = true
		} else if v.CommittedCleared {
			m[k] = false
		}
	}
	return m
}

func (ks WriteKeyInfos) ContainsWrittenKey(key string) bool {
	_, ok := ks.keys[key]
	return ok
}

func (ks WriteKeyInfos) IsWrittenKeyRollbackedCheckMustNotCommitted(key string) bool {
	v := ks.keys[key]
	assert.Must(!v.Committed)
	return v.Rollbacked
}

func (ks WriteKeyInfos) IsWrittenKeyDetermined(key string) bool {
	v := ks.keys[key]
	return v.Rollbacked || v.Committed
}

//func (ks WriteKeyInfos) MustContainWrittenKeys(keys map[string]uint8) {
//	for key := range keys {
//		assert.Must(ks.ContainsWrittenKey(key))
//	}
//}

func (ks WriteKeyInfos) MatchAllWrittenKeyInternalVersions(key2InternalVersion map[string]uint8) bool {
	for k, internalVersion := range key2InternalVersion {
		if ks.keys[k].InternalVersion != internalVersion {
			return false
		}
	}
	return true
}

// Update key info methods, no need to pass pointer

func (ks WriteKeyInfos) MarkAllWrittenKeyCommitted() {
	for k, v := range ks.keys {
		v.Committed = true
		ks.keys[k] = v
	}
}

func (ks WriteKeyInfos) MarkWrittenKeyRollbacked(key string) {
	v, ok := ks.keys[key]
	assert.Must(ok && !v.Committed && v.ValidFlag > 0)
	v.Rollbacked = true

	ks.keys[key] = v
}

func (ks WriteKeyInfos) MarkCommittedCleared(key string, value types.Value) {
	assert.Must(!value.HasWriteIntent())
	v, ok := ks.keys[key]
	assert.Must(ok && v.ValidFlag > 0)
	assert.Must(v.InternalVersion == 0 || v.InternalVersion == value.InternalVersion)
	v.InternalVersion = value.InternalVersion
	if v.LastTask != nil {
		assert.Must(v.LastTask.Data.(uint8) == value.InternalVersion)
	}
	v.Committed = true
	v.CommittedCleared = true

	ks.keys[key] = v
}

// Below are write methods, need pass pointers

func (ks *WriteKeyInfos) WriteKey(taskId string, s *scheduler.ConcurrentDynamicListScheduler, key string, val types.Value, opt types.KVCCWriteOption) error {
	var v = NewWriteKeyInfo(0)
	if ks.keys == nil {
		ks.keys = map[string]WriteKeyInfo{}
	} else {
		v = ks.keys[key]
		v.ValidFlag = 1
	}
	v.InternalVersion++
	val = val.WithInternalVersion(v.InternalVersion)
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
