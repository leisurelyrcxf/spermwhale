package transaction

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/kv/impl/memory"
	"github.com/leisurelyrcxf/spermwhale/testutils"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

func TestTransaction_SetTxnRecord(t *testing.T) {
	testutils.RunTestForNRounds(t, 10000, testTransactionSetTxnRecord)
}

func testTransactionSetTxnRecord(t types.T) (b bool) {
	assert := types.NewAssertion(t)
	ctx := context.Background()

	db := memory.NewMemoryDB()
	txn := newTransaction(1, db, nil)

	var (
		readVal []byte
		readErr error
		wg      sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()

		time.Sleep(utils.RandomPeriod(time.Microsecond, 10, 500))
		val, err := txn.GetTxnRecord(ctx, types.NewKVCCReadOption(types.MaxTxnVersion).WithCheckTxnRecord(1))
		if errors.IsNotExistsErr(err) {
			readErr = err
			return
		}
		if !assert.NoError(err) {
			return
		}
		readVal = val.V
	}()

	for i := 0; i < 10000; i++ {
		err := txn.SetTxnRecord(ctx, types.NewTxnValue([]byte{byte(i)}, txn.ID.Version()).WithInternalVersion(types.TxnInternalVersion(i)), types.NewKVCCWriteOption())
		if err == nil {
			continue
		}
		if !assert.Equal(errors.ErrWriteReadConflict, err) {
			return
		}
	}

	wg.Wait()
	val, err := txn.GetTxnRecord(ctx, types.NewKVCCReadOption(types.MaxTxnVersion).WithCheckTxnRecord(1))
	if readErr != nil {
		if !assert.Equal(readErr, err) {
			return
		}
	} else {
		if !assert.Equal(readVal, val.V) {
			return
		}
	}
	return true
}

type TestDB struct {
	types.KV
}

func newTestDB() *TestDB {
	return &TestDB{KV: memory.NewMemoryDB()}
}

func (db *TestDB) Get(ctx context.Context, key string, opt types.KVReadOption) (types.Value, error) {
	time.Sleep(utils.RandomPeriod(time.Microsecond, 20, 30))
	return db.KV.Get(ctx, key, opt)
}

func (db *TestDB) Set(ctx context.Context, key string, val types.Value, opt types.KVWriteOption) error {
	time.Sleep(utils.RandomPeriod(time.Microsecond, 10, 30))
	return db.KV.Set(ctx, key, val, opt)
}

func TestTransaction_SetTxnRecordGetMaxReadVersion(t *testing.T) {
	testutils.RunTestForNRounds(t, 100000, testTransactionSetTxnRecordGetMaxReadVersion)
}

func testTransactionSetTxnRecordGetMaxReadVersion(t types.T) (b bool) {
	assert := types.NewAssertion(t)
	ctx := context.Background()

	db := newTestDB()
	txn := newTransaction(1, db, nil)

	var (
		readVal []byte
		readErr error
		wg      sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()

		for i := 0; ; i++ {
			if val, err := txn.GetTxnRecord(ctx, types.NewKVCCReadOption(types.MaxTxnVersion).WithCheckTxnRecord(1).
				WithNotUpdateTimestampCache()); val.MaxReadVersion > txn.ID.Version() {
				if errors.IsNotExistsErr(err) {
					readErr = err
					return
				}
				if !assert.NoError(err) {
					return
				}
				readVal = val.V
				return
			}
			time.Sleep(time.Microsecond)
		}
	}()

	go func() {
		defer wg.Done()

		time.Sleep(utils.RandomPeriod(time.Microsecond, 500, 600))
		if _, err := txn.GetTxnRecord(ctx, types.NewKVCCReadOption(types.MaxTxnVersion).WithCheckTxnRecord(1)); !errors.IsNotExistsErr(err) {
			assert.NoError(err)
		}
	}()

	for i := 1; i < 255; i++ {
		err := txn.SetTxnRecord(ctx, types.NewTxnValue([]byte{byte(i)}, txn.ID.Version()).WithInternalVersion(types.TxnInternalVersion(i)), types.NewKVCCWriteOption())
		if err == nil {
			continue
		}
		if !assert.Equal(errors.ErrWriteReadConflict, err) {
			return
		}
	}

	wg.Wait()
	val, err := txn.GetTxnRecord(ctx, types.NewKVCCReadOption(types.MaxTxnVersion).WithCheckTxnRecord(1))
	if readErr != nil {
		if !assert.Equalf(readErr, err, "expect not exists, but got value '%s'", string(val.V)) {
			return
		}
	} else {
		if !assert.Equal(readVal, val.V) {
			return
		}
	}

	//for _, ws := range writeTimestamps {
	//	if !assert.Less(ws, readTimestamp) {
	//		return
	//	}
	//}
	return true
}
