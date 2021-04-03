package kvcc

import (
	"sync"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/basic"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

func TestTxnRecordCC(t *testing.T) {
	for i := 0; i < 1; i++ {
		if !testTxnRecordCC(t) {
			t.Errorf("%s failed @round%d", t.Name(), i)
			return
		}
	}
}

func testTxnRecordCC(_ *testing.T) (b bool) {

	var (
		lm    = concurrency.NewBasicTxnLockManager()
		txnId = types.TxnId(111)
		stop  basic.AtomicBool
		m     = make(map[string]string)
	)

	go func() {
		time.Sleep(utils.RandomPeriod(time.Microsecond, 100, 200))
		stop.Set(true)
	}()

	var (
		wg sync.WaitGroup
	)
	const goRoutineNumPerKind = 100000
	wg.Add(2 * goRoutineNumPerKind)
	for i := 0; i < goRoutineNumPerKind; i++ {
		go func() {
			defer wg.Done()

			if !stop.Get() {
				time.Sleep(time.Nanosecond * 10000)
				lm.Lock(txnId)
				defer lm.Unlock(txnId)

				//if !stop.Get() {
				m["k"] = "v"
				//}
			}

		}()
	}

	for i := 0; i < goRoutineNumPerKind; i++ {
		go func() {
			defer wg.Done()

			lm.RLock(txnId)
			defer lm.RUnlock(txnId)

			if stop.Get() {
				m = nil
				return
			}
		}()
	}

	wg.Wait()
	return true
}
