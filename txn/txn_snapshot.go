package txn

import (
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

func (txn *Txn) addSnapshotReadValue(key string, value types.ValueCC) {
	if txn.readForWriteOrSnapshotReadKeys == nil {
		txn.readForWriteOrSnapshotReadKeys = map[string]types.ValueCC{key: value}
		return
	}
	txn.readForWriteOrSnapshotReadKeys[key] = value
	txn.assertSnapshot()
	return
}

func (txn *Txn) addSnapshotReadValues(keys []string, values []types.ValueCC) error {
	if txn.readForWriteOrSnapshotReadKeys == nil {
		txn.readForWriteOrSnapshotReadKeys = map[string]types.ValueCC{}
	}
	for idx, key := range keys {
		txn.readForWriteOrSnapshotReadKeys[key] = values[idx]
	}
	txn.assertSnapshot()
	return nil
}

func (txn *Txn) assertSnapshot() {
	if txn.SnapshotVersion < txn.getMaxReadVersion() {
		panic("expect txn.SnapshotVersion >= txn.getMaxReadVersion(), but got false")
	}
}

func (txn *Txn) setSnapshotVersion(v uint64) {
	txn.SnapshotVersion = v
	txn.explicitSnapshotVersion = true
}

func (txn *Txn) getSnapshotReadOptionUnsafe() types.KVCCReadOption {
	return types.NewKVCCReadOption(0).WithSnapshotRead(txn.SnapshotVersion, 0)
}

func (txn *Txn) getSnapshotReadOption() types.KVCCReadOption {
	return types.NewKVCCReadOption(0).WithSnapshotRead(txn.SnapshotVersion, txn.getMinAllowedSnapshotReadOption())
}

func (txn *Txn) getMinAllowedSnapshotReadOption() uint64 {
	if txn.explicitSnapshotVersion {
		return txn.SnapshotVersion
	}
	return txn.getMaxReadVersion()
}

func (txn *Txn) getMaxReadVersion() uint64 {
	maxReadVersion := uint64(0)
	for _, v := range txn.readForWriteOrSnapshotReadKeys {
		maxReadVersion = utils.MaxUint64(maxReadVersion, v.Version)
	}
	return maxReadVersion
}
