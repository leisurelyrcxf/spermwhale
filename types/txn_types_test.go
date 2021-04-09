package types

import (
	"sync"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/types/basic"
	testifyassert "github.com/stretchr/testify/assert"
)

const AtomicTxnStateTestRounds = 1000000

func TestAtomicTxnState_SetTxnState(t *testing.T) {
	for i := 0; i < AtomicTxnStateTestRounds; i++ {
		if !testAtomicTxnStateSetTxnState(t) {
			t.Errorf("%s failed @round %d", t.Name(), i)
			return
		}
	}
}

func testAtomicTxnStateSetTxnState(t *testing.T) (b bool) {
	s := NewAtomicTxnState(TxnStateUncommitted)

	const (
		goRoutineNumber = 10
	)

	var (
		wg              sync.WaitGroup
		terminatedTimes basic.AtomicInt32
	)
	wg.Add(goRoutineNumber)
	for i := 0; i < goRoutineNumber; i++ {
		go func() {
			defer wg.Done()

			if _, terminateOnce := s.SetTxnState(TxnStateRollbacking); terminateOnce {
				terminatedTimes.Add(1)
			}
		}()
	}
	wg.Wait()

	return NewAssertion(t).Equal(int32(1), terminatedTimes.Get())
}

func TestAtomicTxnState_SetTxnStateUnsafe(t *testing.T) {
	for i := 0; i < AtomicTxnStateTestRounds; i++ {
		if !testAtomicTxnStateSetTxnStateUnsafe(t) {
			t.Errorf("%s failed @round %d", t.Name(), i)
			return
		}
	}
}

func testAtomicTxnStateSetTxnStateUnsafe(t *testing.T) (b bool) {
	s := NewAtomicTxnState(TxnStateUncommitted)

	const (
		goRoutineNumber = 10
	)

	var (
		wg              sync.WaitGroup
		terminatedTimes basic.AtomicInt32
		mu              sync.Mutex
	)
	wg.Add(goRoutineNumber)
	for i := 0; i < goRoutineNumber; i++ {
		go func() {
			defer wg.Done()

			mu.Lock()
			if _, terminateOnce := s.SetTxnStateUnsafe(TxnStateRollbacking); terminateOnce {
				terminatedTimes.Add(1)
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	return NewAssertion(t).Equal(int32(1), terminatedTimes.Get())
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
