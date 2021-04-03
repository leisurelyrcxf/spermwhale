package txn

import (
	"context"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/kv/impl/memory"
	"github.com/leisurelyrcxf/spermwhale/kvcc"
	"github.com/leisurelyrcxf/spermwhale/txn/smart_txn_client"
	"github.com/leisurelyrcxf/spermwhale/txn/ttypes"
	"github.com/leisurelyrcxf/spermwhale/types"
)

func TestTxnLostUpdate(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnLostUpdate).SetStaleWriteThreshold(time.Millisecond * 10).Run()
}
func TestTxnLostUpdateReadModifyWrite(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnLostUpdate).SetTxnType(types.TxnTypeReadModifyWrite).Run()
}
func TestTxnLostUpdateWaitWhenReadDirty(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnLostUpdate).SetTxnType(types.TxnTypeWaitWhenReadDirty).SetStaleWriteThreshold(time.Millisecond * 10).Run()
}
func TestTxnLostUpdateReadModifyWriteWaitWhenReadDirty(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnLostUpdate).SetTxnType(types.TxnTypeReadModifyWrite | types.TxnTypeWaitWhenReadDirty).Run()
}
func TestTxnLostUpdateInjectErr(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnLostUpdate).SetStaleWriteThreshold(time.Millisecond * 10).SetStaleWriteThreshold(time.Millisecond * 10).
		SetGoRoutineNum(100).SetFailurePattern(FailurePatternAll).SetFailureProbability(10).Run()
}

func TestTxnLostUpdateWriteAfterWrite(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnLostUpdateWriteAfterWrite).SetStaleWriteThreshold(time.Millisecond * 10).Run()
}

func TestTxnReadModifyWrite2Keys(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2Keys).Run()
}
func TestTxnReadModifyWrite2KeysWaitWhenReadDirty(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2Keys).SetTxnType(types.TxnTypeWaitWhenReadDirty).Run()
}
func TestTxnReadModifyWrite2KeysReadModifyWrite(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2Keys).SetTxnType(types.TxnTypeReadModifyWrite).Run()
}
func TestTxnReadModifyWrite2KeysReadModifyWriteWaitWhenReadDirty(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2Keys).SetTxnType(types.TxnTypeReadModifyWrite | types.TxnTypeWaitWhenReadDirty).Run()
}

func TestTxnReadModifyWrite2KeysMGetMSet(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2KeysMGetMSet).Run()
}
func TestTxnReadModifyWrite2KeysMGetMSetWaitWhenReadDirty(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2KeysMGetMSet).SetTxnType(types.TxnTypeWaitWhenReadDirty).Run()
}
func TestTxnReadModifyWrite2KeysMGetMSetReadModifyWrite(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2KeysMGetMSet).SetTxnType(types.TxnTypeReadModifyWrite).Run()
}
func TestTxnReadModifyWrite2KeysMGetMSetReadModifyWriteWaitWhenReadDirty(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2KeysMGetMSet).SetTxnType(types.TxnTypeReadModifyWrite | types.TxnTypeWaitWhenReadDirty).Run()
}

func TestTxnReadModifyWrite2KeysDeadlock(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2KeysDeadlock).Run()
}
func TestTxnReadModifyWrite2KeysDeadlockReadModifyWrit(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2KeysDeadlock).SetTxnType(types.TxnTypeReadModifyWrite).Run()
}
func TestTxnReadModifyWrite2KeysDeadlockWaitWhenReadDirty(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2KeysDeadlock).SetTxnType(types.TxnTypeWaitWhenReadDirty).Run()
}
func TestTxnReadModifyWrite2KeysDeadlockReadModifyWriteWaitWhenReadDirty(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2KeysDeadlock).SetTxnType(types.TxnTypeReadModifyWrite | types.TxnTypeWaitWhenReadDirty).Run()
}

func TestTxnReadModifyWrite2KeysDeadlockMGetMSet(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2KeysDeadlockMGetMSet).Run()
}
func TestTxnReadModifyWrite2KeysDeadlockMGetMSetReadModifyWrit(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2KeysDeadlockMGetMSet).SetTxnType(types.TxnTypeReadModifyWrite).Run()
}
func TestTxnReadModifyWrite2KeysDeadlockMGetMSetWaitWhenReadDirty(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2KeysDeadlockMGetMSet).SetTxnType(types.TxnTypeWaitWhenReadDirty).Run()
}
func TestTxnReadModifyWrite2KeysDeadlockMGetMSetReadModifyWriteWaitWhenReadDirty(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2KeysDeadlockMGetMSet).SetTxnType(types.TxnTypeReadModifyWrite | types.TxnTypeWaitWhenReadDirty).Run()
}

func TestTxnReadModifyWriteNKeys(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWriteNKeys).SetTxnType(types.TxnTypeReadModifyWrite | types.TxnTypeWaitWhenReadDirty).Run()
}

func TestTxnLostUpdateModAdd(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnLostUpdateModAdd).SetTxnType(types.TxnTypeReadModifyWrite | types.TxnTypeWaitWhenReadDirty).Run()
}

func TestTxnReadWriteAfterWrite(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadWriteAfterWrite).Run()
}
func TestTxnReadWriteAfterWriteReadModifyWriteWaitWhenReadDirty(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadWriteAfterWrite).SetTxnType(types.TxnTypeReadModifyWrite | types.TxnTypeWaitWhenReadDirty).Run()
}
func TestTxnReadWriteAfterWriteMSet(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadWriteAfterWriteMSet).Run()
}
func TestTxnReadWriteAfterWriteReadModifyWriteWaitWhenReadDirtyMSet(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadWriteAfterWriteMSet).SetTxnType(types.TxnTypeReadModifyWrite | types.TxnTypeWaitWhenReadDirty).Run()
}

func TestTxnLostUpdateWithSomeAbortedCommitFailed(t *testing.T) {
	NewMaliciousEmbeddedTestCase(t, rounds, func(ctx context.Context, testCase *TestCase) bool {
		return testTxnLostUpdateWithSomeAbortedCommitFailed(ctx, testCase, 100)
	}).Run()
}
func TestTxnLostUpdateWithSomeAbortedCommitFailedReadModifyWrite(t *testing.T) {
	NewMaliciousEmbeddedTestCase(t, rounds, func(ctx context.Context, testCase *TestCase) bool {
		return testTxnLostUpdateWithSomeAbortedCommitFailed(ctx, testCase, 1000)
	}).SetTxnType(types.TxnTypeReadModifyWrite).
		SetTimeoutPerRound(time.Minute * 1).SetMaxRetryPerTxn(10000).Run()
}
func TestTxnLostUpdateWithSomeAbortedCommitFailedWaitWhenReadDirty(t *testing.T) {
	NewMaliciousEmbeddedTestCase(t, rounds, func(ctx context.Context, testCase *TestCase) bool {
		return testTxnLostUpdateWithSomeAbortedCommitFailed(ctx, testCase, 1000)
	}).SetTxnType(types.TxnTypeWaitWhenReadDirty).
		SetTimeoutPerRound(time.Minute * 1).SetMaxRetryPerTxn(10000).Run()
}
func TestTxnLostUpdateWithSomeAbortedCommitFailedReadModifyWriteWaitWhenReadDirty(t *testing.T) {
	NewMaliciousEmbeddedTestCase(t, rounds, func(ctx context.Context, testCase *TestCase) bool {
		return testTxnLostUpdateWithSomeAbortedCommitFailed(ctx, testCase, 1000)
	}).SetTxnType(types.TxnTypeReadModifyWrite | types.TxnTypeWaitWhenReadDirty).SetMaxRetryPerTxn(10000).Run()
}

func TestTxnLostUpdateWithSomeAbortedRollbackFailed(t *testing.T) {
	NewMaliciousEmbeddedTestCase(t, rounds, func(ctx context.Context, testCase *TestCase) bool {
		return testTxnLostUpdateWithSomeAbortedRollbackFailed(ctx, testCase, 100)
	}).Run()
}
func TestTxnLostUpdateWithSomeAbortedRollbackFailedReadModifyWrite(t *testing.T) {
	NewMaliciousEmbeddedTestCase(t, rounds, func(ctx context.Context, testCase *TestCase) bool {
		return testTxnLostUpdateWithSomeAbortedRollbackFailed(ctx, testCase, 1)
	}).SetTxnType(types.TxnTypeReadModifyWrite).
		SetTimeoutPerRound(time.Minute * 10).SetMaxRetryPerTxn(10000).Run()
}
func TestTxnLostUpdateWithSomeAbortedRollbackFailedWaitWhenReadDirty(t *testing.T) {
	NewMaliciousEmbeddedTestCase(t, rounds, func(ctx context.Context, testCase *TestCase) bool {
		return testTxnLostUpdateWithSomeAbortedRollbackFailed(ctx, testCase, 100)
	}).SetTxnType(types.TxnTypeWaitWhenReadDirty).
		SetTimeoutPerRound(time.Minute * 10).SetMaxRetryPerTxn(10000).Run()
}
func TestTxnLostUpdateWithSomeAbortedRollbackFailedReadModifyWriteWaitWhenReadDirty(t *testing.T) {
	NewMaliciousEmbeddedTestCase(t, rounds, func(ctx context.Context, testCase *TestCase) bool {
		return testTxnLostUpdateWithSomeAbortedRollbackFailed(ctx, testCase, 1)
	}).SetTxnType(types.TxnTypeReadModifyWrite | types.TxnTypeWaitWhenReadDirty).
		SetLogLevel(10).Run()
}

func TestTxnLostUpdateWriteAfterWriteOverflow(t *testing.T) {
	const (
		initialValue    = 101
		goRoutineNumber = 10000
		key             = "k1"
	)
	if goRoutineNumber&1 != 0 {
		panic("goRoutineNumber&1 != 0")
	}
	db := memory.NewMemoryDB()
	kvc := kvcc.NewKVCCForTesting(db, defaultTabletTxnConfig.WithStaleWriteThreshold(time.Second))
	m := NewTransactionManager(kvc, defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(time.Second))
	sc := smart_txn_client.NewSmartClient(m, 0)
	defer sc.Close()
	assert := types.NewAssertion(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	assert.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
		for i := 0; i < consts.MaxTxnInternalVersion; i++ {
			if err := txn.Set(ctx, key, types.NewIntValue(initialValue).V); err != nil {
				return err
			}
		}
		return nil
	}))
	val, err := sc.Get(ctx, key)
	assert.NoError(err)
	assert.Equal(types.TxnInternalVersionMax, val.InternalVersion)
	assert.Equal(types.TxnInternalVersion(254), val.InternalVersion)
	assert.Equal(errors.ErrTransactionInternalVersionOverflow, sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
		for i := 0; i < consts.PositiveInvalidTxnInternalVersion; i++ {
			if err := txn.Set(ctx, key, types.NewIntValue(initialValue).V); err != nil {
				return err
			}
		}
		return nil
	}))
}

func TestTxnEncode(t *testing.T) {
	assert := types.NewAssertion(t)

	txn := NewTxn(123, types.TxnTypeReadModifyWrite, kvcc.NewKVCCForTesting(memory.NewMemoryDB(), defaultTabletTxnConfig), defaultTxnManagerConfig, &TransactionStore{}, nil, nil)
	txn.TxnState = types.TxnStateRollbacking
	txn.InitializeWrittenKeys(ttypes.KeyVersions{"k1": 111, "k2": 222}, true)
	bytes := txn.Encode()
	t.Logf("txn:     %s", string(bytes))

	newTxn, err := DecodeTxn(123, bytes)
	assert.NoError(err)
	t.Logf("new_txn: %s", string(newTxn.Encode()))
	assert.Equal(txn.ID, newTxn.ID)
	assert.Equal(txn.TxnType, newTxn.TxnType)
	assert.Equal(txn.TxnState, newTxn.TxnState)
	assert.Equal(txn.GetWrittenKey2LastVersion(), newTxn.GetWrittenKey2LastVersion())
}
