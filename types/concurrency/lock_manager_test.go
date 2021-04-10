package concurrency

import (
	"sync"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/leisurelyrcxf/spermwhale/testutils"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

func TestAdvancedTxnLockManager_Lock(t *testing.T) {
	testutils.RunTestForNRounds(t, 100, testAdvancedTxnLockManagerLock)
}

func testAdvancedTxnLockManagerLock(t types.T) (b bool) {
	const GoRoutineNumber = 10000

	var (
		assert = testifyassert.New(t)

		lm = NewAdvancedTxnLockManager(64)
		wg sync.WaitGroup
		j  = 0
	)

	for i := 0; i < GoRoutineNumber; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			lm.Lock(types.MaxTxnId)
			defer lm.Unlock(types.MaxTxnId)

			j++
		}()
	}
	wg.Wait()

	for idx := range lm.partitions {
		if !assert.Empty(lm.partitions[idx].m) {
			return
		}
	}
	return assert.Equal(GoRoutineNumber, j)
}

func TestAdvancedTxnLockManager_RLock(t *testing.T) {
	testutils.RunTestForNRounds(t, 100, testAdvancedTxnLockManagerRLock)
}

func testAdvancedTxnLockManagerRLock(t types.T) (b bool) {
	const (
		GoRoutineNumber = 100
		TxnNumber       = 1000
	)

	type Txn struct {
		id      types.TxnId
		i, j, k int
	}

	var (
		assert = testifyassert.New(t)
		lm     = NewAdvancedTxnLockManager(64)
		wg     sync.WaitGroup
		txns   = [TxnNumber]*Txn{}
	)
	if !assert.Len(txns, TxnNumber) {
		return
	}
	for i := range txns {
		txns[i] = &Txn{id: types.TxnId(i)}
	}

	for r := 0; r < GoRoutineNumber; r++ {
		for x := 0; x < TxnNumber; x++ {
			wg.Add(1)
			go func(x int) {
				defer wg.Done()
				var txn = txns[x]

				lm.Lock(txn.id)
				defer lm.Unlock(txn.id)

				txn.i++
				txn.j = txn.i * 10
				txn.k = txn.i * 100
			}(x)
		}
	}

	for r := 0; r < GoRoutineNumber; r++ {
		for x := 0; x < TxnNumber; x++ {
			wg.Add(1)
			go func(x int) {
				defer wg.Done()
				var txn = txns[x]

				lm.RLock(txn.id)
				defer lm.RUnlock(txn.id)

				assert.Equal(txn.i*10, txn.j)
				assert.Equal(txn.i*100, txn.k)
			}(x)
		}
	}

	wg.Wait()

	for _, txn := range txns {
		if !assert.Equal(GoRoutineNumber, txn.i) && assert.Equal(GoRoutineNumber*10, txn.j) && assert.Equal(GoRoutineNumber*100, txn.k) {
			return
		}
	}
	for idx := range lm.partitions {
		if !assert.Empty(lm.partitions[idx].m) {
			return
		}
	}
	return true
}

const (
	shortReadTaskLen = time.Microsecond * 20
	longReadTaskLen  = time.Millisecond * 5
)

func TestBasicTxnLockManagerShortTask(t *testing.T) {
	testutils.RunTestForNRounds(t, 1, func(t types.T) (b bool) {
		return benchTxnLockManager(t, NewBasicTxnLockManager(), shortReadTaskLen)
	})
}

func TestAdvancedTxnLockManagerShortTask(t *testing.T) {
	testutils.RunTestForNRounds(t, 1, func(t types.T) (b bool) {
		return benchTxnLockManager(t, NewAdvancedTxnLockManager(64), shortReadTaskLen)
	})
}

func TestBasicTxnLockManagerLongTask(t *testing.T) {
	testutils.RunTestForNRounds(t, 1, func(t types.T) (b bool) {
		return benchTxnLockManager(t, NewBasicTxnLockManager(), longReadTaskLen)
	})
}

func TestAdvancedTxnLockManagerLongTask(t *testing.T) {
	testutils.RunTestForNRounds(t, 1, func(t types.T) (b bool) {
		return benchTxnLockManager(t, NewAdvancedTxnLockManager(64), longReadTaskLen)
	})
}

func benchTxnLockManager(t types.T, lm TxnLockManager, readerTaskLen time.Duration) (b bool) {
	const (
		GoRoutineNumber = 10000
		WriterNum       = 50
		ReaderNum       = 10
	)

	var (
		writerTaskLen = readerTaskLen * 5

		wg sync.WaitGroup

		writerSequences = [WriterNum][]int{}
		readerSequences = [ReaderNum][]int{}
	)
	for idx := range writerSequences {
		writerSequences[idx] = utils.RandomSequence(GoRoutineNumber)
	}
	for idx := range readerSequences {
		readerSequences[idx] = utils.RandomSequence(GoRoutineNumber)
	}

	for _, seq := range writerSequences {
		for _, r := range seq {
			wg.Add(1)
			go func(r int) {
				defer wg.Done()

				lm.Lock(types.TxnId(r))
				defer lm.Unlock(types.TxnId(r))

				time.Sleep(writerTaskLen)
			}(r)
		}
	}

	for _, seq := range readerSequences {
		for _, r := range seq {
			wg.Add(1)
			go func(r int) {
				defer wg.Done()

				lm.RLock(types.TxnId(r))
				defer lm.RUnlock(types.TxnId(r))

				time.Sleep(readerTaskLen)
			}(r)
		}
	}
	wg.Wait()

	assert := types.NewAssertion(t)
	if lm, ok := lm.(*AdvancedTxnLockManager); ok {
		for idx := range lm.partitions {
			if !assert.Empty(lm.partitions[idx].m) {
				return
			}
		}
	}
	return true
}
