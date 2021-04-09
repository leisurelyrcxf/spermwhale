package concurrency

import (
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/consts"
	testifyassert "github.com/stretchr/testify/assert"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

type TestTxn struct {
	sync.Mutex

	Future

	types.TxnState
}

func newTestTxn() *TestTxn {
	return &TestTxn{
		Future:   *NewFuture(),
		TxnState: types.TxnStateUncommitted,
	}
}

var errTerminated = fmt.Errorf("terminated")

func (txn *TestTxn) Add(key string) error {
	txn.Lock()
	defer txn.Unlock()

	if txn.IsTerminated() {
		return errTerminated
	}
	added, done := txn.Future.add(key, types.DBMeta{})
	if added {
		glog.V(120).Infof("[TestTxn::Add] inserted key '%s'", key)
	}
	assert.Must(!done)
	return nil
}

func (txn *TestTxn) Done(key string) bool {
	txn.Lock()
	defer txn.Unlock()

	//if txn.IsTerminated() {
	//	return true
	//}
	doneOnce, done := txn.Future.doneUnsafeEx(key)
	if done {
		for key, done := range txn.Future.keys {
			assert.Failf(done.Done, "'%s' not done yet", key)
		}
		glog.V(120).Infof("[TestTxn::Done] txn done after done key '%s'", key)
	}
	if doneOnce {
		glog.V(120).Infof("[TestTxn::Done] done key '%s'", key)
	}
	txn.TxnState = types.TxnStateRollbacking
	return done
}

const rounds = 1000

func TestDoneSet(t *testing.T) {
	testDoneSet(t, false)
}

func TestDoneSetSleep(t *testing.T) {
	testDoneSet(t, true)
}

func testDoneSet(t *testing.T, sleep bool) {
	_ = flag.Set("alsologtostderr", fmt.Sprintf("%t", true))
	testifyassert.NoError(t, utils.MkdirIfNotExists(consts.DefaultTestLogDir))
	_ = flag.Set("v", fmt.Sprintf("%d", 50))
	_ = flag.Set("log_dir", consts.DefaultTestLogDir)

	for i := 0; i < rounds; i++ {
		if !testDoneSetFunc(t, sleep) {
			t.Errorf("%s failed @round %d", t.Name(), i)
			return
		}
		t.Logf("%s succeeded @round %d, sleep: %v", t.Name(), i, sleep)
	}
}

func testDoneSetFunc(t *testing.T, sleep bool) (b bool) {
	var (
		ts = types.NewAssertion(t)

		txn      = newTestTxn()
		keys     = []string{"k1", "k2", "k3"}
		doneKeys = append(append([]string{}, keys...), "k4")

		wg sync.WaitGroup
	)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			rand.Seed(time.Now().UnixNano())
			key := keys[rand.Intn(len(keys))]
			if err := txn.Add(key); err != nil {
				return
			}
		}()
	}

	if sleep {
		time.Sleep(utils.RandomPeriod(time.Nanosecond, 0, 10))
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			rand.Seed(time.Now().UnixNano())
			key := doneKeys[rand.Intn(len(doneKeys))]
			txn.Done(key)
		}(i)
	}

	wg.Wait()
	for key, done := range txn.Future.keys {
		if !ts.Truef(done.Done, "'%s' not done yet", key) {
			return
		}
	}
	return true
}
