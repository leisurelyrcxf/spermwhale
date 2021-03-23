package txn

import (
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/types"
)

func TestTxnLostUpdateMongo(t *testing.T) {
	testTxnLostUpdateMongo(t, types.TxnTypeDefault, types.NewTxnReadOption(), []time.Duration{time.Second * 10})
}

func TestTxnLostUpdateMongoReadForWrite(t *testing.T) {
	testTxnLostUpdateMongo(t, types.TxnTypeReadForWrite, types.NewTxnReadOption(), []time.Duration{time.Second * 10})
}

func TestTxnLostUpdateMongoWaitNoWriteIntent(t *testing.T) {
	testTxnLostUpdateMongo(t, types.TxnTypeDefault, types.NewTxnReadOption().WithWaitNoWriteIntent(), []time.Duration{time.Second * 10})
}

func TestTxnLostUpdateMongoReadForWriteWaitNoWriteIntent(t *testing.T) {
	testTxnLostUpdateMongo(t, types.TxnTypeReadForWrite, types.NewTxnReadOption().WithWaitNoWriteIntent(), []time.Duration{time.Second * 10})
}

func TestTxnLostUpdateMongoRandomErr(t *testing.T) {
	testTxnLostUpdateRaw(t, types.DBTypeMongo, 100, types.TxnTypeDefault, types.NewTxnReadOption(), []time.Duration{time.Millisecond * 10},
		FailurePatternAll, 10)
}

func testTxnLostUpdateMongo(t *testing.T, txnType types.TxnType, readOpt types.TxnReadOption, staleWriteThresholds []time.Duration) {
	testTxnLostUpdateRaw(t, types.DBTypeMongo, 100, txnType, readOpt, staleWriteThresholds, FailurePatternNone, 0)
}

func TestDistributedTxnConsistencyIntegrateMongo(t *testing.T) {
	testDistributedTxnConsistencyIntegrate(NewTestCase(t).SetDBType(types.DBTypeMongo))
}

func TestDistributedTxnConsistencyIntegrateMongoReadForWriteWaitNoWriteIntent(t *testing.T) {
	testDistributedTxnConsistencyIntegrate(NewTestCase(t).SetDBType(types.DBTypeMongo).SetTxnType(types.TxnTypeReadForWrite).SetWaitNoWriteIntent())
}
