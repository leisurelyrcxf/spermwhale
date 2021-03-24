package ttypes

import (
	"fmt"
	"testing"

	"github.com/leisurelyrcxf/spermwhale/types"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestEmptyWriteKeyInfo(t *testing.T) {
	testifyassert.True(t, EmptyWriteKeyInfo.IsEmpty())
	testifyassert.False(t, NewWriteKeyInfo(1).IsEmpty())
	testifyassert.False(t, NewWriteKeyInfo(255).IsEmpty())
}

func TestWriteKeyInfos_GetLastWriteKeyTasks(t *testing.T) {
	keys := WriteKeyInfos{}
	keys.InitializeWrittenKeys(map[string]types.TxnInternalVersion{"k1": 123, "k2": 224, "k3": 100}, true)
	keys.tasks = []*types.ListTask{
		{ID: types.ListTaskID{Key: "k1"}},
		{ID: types.ListTaskID{Key: "k1"}},
		{ID: types.ListTaskID{Key: "k2"}},
		{ID: types.ListTaskID{Key: "k3"}},
	}
	keys.setLastTask("k1", keys.tasks[1])
	keys.setLastTask("k2", keys.tasks[2])
	keys.setLastTask("k3", keys.tasks[3])

	_, tasks := keys.GetCopiedWriteKeyTasksEx()
	testifyassert.Len(t, tasks, 4)
	lastTasks := keys.GetLastWriteKeyTasks(tasks)
	testifyassert.Len(t, lastTasks, 3)
}

func TestWriteKeyInfos_GetLastWriteKeyTasks2(t *testing.T) {
	keys := WriteKeyInfos{}
	keys.InitializeWrittenKeys(map[string]types.TxnInternalVersion{"k1": 123, "k2": 224, "k3": 100}, true)
	keys.tasks = []*types.ListTask{
		{ID: types.ListTaskID{Key: "k1"}},
		{ID: types.ListTaskID{Key: "k2"}},
		{ID: types.ListTaskID{Key: "k3"}},
	}
	for idx := range keys.tasks {
		keys.tasks[idx].Task.ID = keys.tasks[idx].ID.Key
	}
	keys.setLastTask("k1", keys.tasks[0])
	keys.setLastTask("k2", keys.tasks[1])
	keys.setLastTask("k3", keys.tasks[2])

	_, tasks := keys.GetCopiedWriteKeyTasksEx()
	testifyassert.Len(t, tasks, 3)
	lastTasks := keys.GetLastWriteKeyTasks(tasks)
	testifyassert.Len(t, lastTasks, 3)

	another := &types.ListTask{ID: types.ListTaskID{Key: "k4"}}
	tasks = append(tasks, another)
	lastTasks = append(lastTasks, another)
	testifyassert.Len(t, tasks, 4)
	testifyassert.Len(t, lastTasks, 4)
	for idx, task := range tasks {
		testifyassert.Equal(t, fmt.Sprintf("k%d", idx+1), task.ID.Key)
	}
	for idx, task := range lastTasks {
		testifyassert.Equal(t, fmt.Sprintf("k%d", idx+1), task.ID.Key)
	}
}
func TestWriteKeyInfos_GetLastWriteKeyTasks3(t *testing.T) {
	keys := WriteKeyInfos{}
	keys.InitializeWrittenKeys(map[string]types.TxnInternalVersion{"k1": 123, "k2": 224, "k3": 100}, true)
	keys.tasks = []*types.ListTask{
		{ID: types.ListTaskID{Key: "k1"}},
		{ID: types.ListTaskID{Key: "k2"}},
		{ID: types.ListTaskID{Key: "k3"}},
	}
	for idx := range keys.tasks {
		keys.tasks[idx].Task.ID = keys.tasks[idx].ID.Key
	}
	keys.setLastTask("k1", keys.tasks[0])
	keys.setLastTask("k2", keys.tasks[1])
	keys.setLastTask("k3", keys.tasks[2])

	_, tasks := keys.GetCopiedWriteKeyTasksEx()
	testifyassert.Len(t, tasks, 3)
	lastTasks := keys.GetLastWriteKeyTasks(tasks)
	testifyassert.Len(t, lastTasks, 3)

	tasks = append(tasks, &types.ListTask{ID: types.ListTaskID{Key: "k4"}})
	lastTasks = append(lastTasks, &types.ListTask{ID: types.ListTaskID{Key: "k5"}})
	testifyassert.Len(t, tasks, 4)
	testifyassert.Len(t, lastTasks, 4)
	for idx, task := range tasks {
		expKeyId := idx + 1
		if idx == 3 {
			expKeyId = 4
		}
		testifyassert.Equal(t, fmt.Sprintf("k%d", expKeyId), task.ID.Key)
	}
	for idx, task := range lastTasks {
		expKeyId := idx + 1
		if idx == 3 {
			expKeyId = 5
		}
		testifyassert.Equal(t, fmt.Sprintf("k%d", expKeyId), task.ID.Key)
	}
}
