package types

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/types/basic"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestAtomicTxnState_SetTxnState(t *testing.T) {
	assert := NewAssertion(t)

	s := NewAtomicTxnState(TxnStateUncommitted)

	const (
		goRoutineNumber = 10000
	)

	var (
		wg              sync.WaitGroup
		terminatedTimes basic.AtomicInt32
	)
	wg.Add(goRoutineNumber)
	for i := 0; i < goRoutineNumber; i++ {
		go func() {
			defer wg.Done()

			if _, _, terminateOnce := s.SetTxnState(TxnStateRollbacking); terminateOnce {
				terminatedTimes.Add(1)
			}
		}()
	}
	wg.Wait()

	assert.Equal(int32(1), terminatedTimes.Get())
}

func TestSafeIncr(t *testing.T) {
	assert := testifyassert.New(t)

	u := uint64(math.MaxUint64)
	assert.Equal(uint64(math.MaxUint64), u)
	SafeIncr(&u)
	assert.Equal(uint64(math.MaxUint64), u)
	u += 1
	assert.Equal(uint64(0), u)
}

func TestMaxTxnVersion(t *testing.T) {
	date, err := time.Parse(time.RFC3339, MaxTxnVersionDate)
	if !testifyassert.NoError(t, err) {
		return
	}
	testifyassert.Equal(t, MaxTxnVersion, uint64(date.UnixNano()))
	testifyassert.Equal(t, uint64(0), MaxTxnVersion&((1<<12)-1))
	t.Logf("max txn version date: %s(%d)", date.String(), date.UnixNano())
}

func TestIsValidTxnInternalVersion(t *testing.T) {
	assert := testifyassert.New(t)

	assert.False(TxnInternalVersion(0).IsValid())
	for v := TxnInternalVersionMin; v <= 254; v++ {
		assert.True(v.IsValid())
	}
	assert.False(TxnInternalVersion(255).IsValid())
}
