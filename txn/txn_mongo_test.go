package txn

import (
	"testing"

	"github.com/leisurelyrcxf/spermwhale/types"
)

//func TestTxnLostUpdateMongo(t *testing.T) {
//	testTxnLostUpdateMongo(t, types.TxnTypeDefault, types.NewTxnReadOption(), []time.Duration{time.Second * 10})
//}
//
//func TestTxnLostUpdateMongoReadModifyWrite(t *testing.T) {
//	testTxnLostUpdateMongo(t, types.TxnTypeReadModifyWrite, types.NewTxnReadOption(), []time.Duration{time.Second * 10})
//}
//
//func TestTxnLostUpdateMongoWaitWhenReadDirty(t *testing.T) {
//	testTxnLostUpdateMongo(t, types.TxnTypeDefault, types.NewTxnReadOption().WithWaitWhenReadDirty(), []time.Duration{time.Second * 10})
//}
//
//func TestTxnLostUpdateMongoReadModifyWriteWaitWhenReadDirty(t *testing.T) {
//	testTxnLostUpdateMongo(t, types.TxnTypeReadModifyWrite, types.NewTxnReadOption().WithWaitWhenReadDirty(), []time.Duration{time.Second * 10})
//}
//
//func TestTxnLostUpdateMongoRandomErr(t *testing.T) {
//	useTxnLostUpdateEmbeddedTabletEx(t, types.DBTypeMongo, 100, types.TxnTypeDefault, types.NewTxnReadOption(), []time.Duration{time.Millisecond * 10},
//		FailurePatternAll, 10)
//
//	useTxnLostUpdateEmbeddedTablet(NewTestCase(t, rounds, testTxnLostUpdate).SetTxnType(types.TxnTypeReadModifyWrite).SetWaitWhenReadDirty().
//		SetGoRoutineNum(100), newTestDB(0, FailurePatternAll, 10))
//}
//
//func testTxnLostUpdateMongo(t *testing.T, txnType types.TxnType, readOpt types.TxnReadOption, staleWriteThresholds []time.Duration) {
//	useTxnLostUpdateEmbeddedTablet(t, types.DBTypeMongo, 100, txnType, readOpt, staleWriteThresholds, FailurePatternNone, 0)
//}
//
//func TestDistributedTxnLostUpdateMongo(t *testing.T) {
//	NewTestCase(t, rounds, testDistributedTxnLostUpdate).SetDBType(types.DBTypeMongo).Run()
//}

func TestDistributedTxnReadConsistencyMongo(t *testing.T) {
	NewTestCase(t, rounds, testDistributedTxnReadConsistency).SetDBType(types.DBTypeMongo).Run()
}

func TestDistributedTxnReadConsistencyDeadlockMongo(t *testing.T) {
	NewTestCase(t, rounds, testDistributedTxnReadConsistencyDeadlock).SetDBType(types.DBTypeMongo).Run()
}

func TestDistributedTxnWriteSkewMongo(t *testing.T) {
	NewTestCase(t, rounds, testDistributedTxnWriteSkew).SetDBType(types.DBTypeMongo).Run()
}

func TestDistributedTxnExtraWriteSimpleMongo(t *testing.T) {
	NewTestCase(t, rounds, testDistributedTxnExtraWriteSimple).SetDBType(types.DBTypeMongo).Run()
}

func TestDistributedTxnExtraWriteComplexMongo(t *testing.T) {
	NewTestCase(t, rounds, testDistributedTxnExtraWriteComplex).SetDBType(types.DBTypeMongo).Run()
}

func TestDistributedTxnExtraWriteSimpleMongoReadModifyWriteWaitWhenReadDirty(t *testing.T) {
	NewTestCase(t, rounds, testDistributedTxnExtraWriteSimple).SetDBType(types.DBTypeMongo).SetTxnType(types.TxnTypeReadModifyWrite | types.TxnTypeWaitWhenReadDirty).Run()
}

func TestDistributedTxnExtraWriteComplexMongoReadModifyWriteWaitWhenReadDirty(t *testing.T) {
	NewTestCase(t, rounds, testDistributedTxnExtraWriteComplex).SetDBType(types.DBTypeMongo).SetTxnType(types.TxnTypeReadModifyWrite | types.TxnTypeWaitWhenReadDirty).Run()
}
