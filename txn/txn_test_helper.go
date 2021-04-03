package txn

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/gate"
	"github.com/leisurelyrcxf/spermwhale/kv/impl/memory"
	"github.com/leisurelyrcxf/spermwhale/kv/impl/mongodb"
	"github.com/leisurelyrcxf/spermwhale/kv/impl/redis"
	"github.com/leisurelyrcxf/spermwhale/kvcc"
	"github.com/leisurelyrcxf/spermwhale/oracle"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"
	"github.com/leisurelyrcxf/spermwhale/topo"
	"github.com/leisurelyrcxf/spermwhale/topo/client"
	"github.com/leisurelyrcxf/spermwhale/txn/smart_txn_client"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

const (
	defaultClusterName = "test_cluster"
	rounds             = 1
)

var (
	defaultTxnManagerConfig = types.NewTxnManagerConfig(time.Millisecond * 10000)
	defaultTabletTxnConfig  = types.NewTabletTxnConfig(time.Millisecond * 10000).WithMaxClockDrift(0)
)

type FailurePattern int

const (
	FailurePatternNone                   = 0
	FailurePatternAckLost FailurePattern = 1 << iota
	FailurePatternReqLost
	FailurePatternAll = 0xffffffff
)

func (p FailurePattern) IsAckLost() bool {
	return p&FailurePatternAckLost == FailurePatternAckLost
}

func (p FailurePattern) IsReqLost() bool {
	return p&FailurePatternReqLost == FailurePatternReqLost
}

type testDB struct {
	types.KV
	latency                                  time.Duration
	randomLatency                            bool
	randomLatencyUnit                        time.Duration
	randomLatencyMin, randomLatencyMax       int
	probabilityOfTxnRecordFailureDenominator int
	failurePattern                           FailurePattern
}

func newTestMemoryDB(latency time.Duration, failurePattern FailurePattern, failureProbability int) *testDB {
	return newTestDB(memory.NewMemoryDB(), latency, failurePattern, failureProbability)
}

func newTestDB(db types.KV, latency time.Duration, failurePattern FailurePattern, failureProbability int) *testDB {
	return &testDB{
		KV:                                       db,
		latency:                                  latency,
		probabilityOfTxnRecordFailureDenominator: failureProbability,
		failurePattern:                           failurePattern,
	}
}

func (d *testDB) SetRandomLatency(randomLatency bool, unit time.Duration, min, max int) *testDB {
	d.randomLatency = randomLatency
	d.randomLatencyUnit = unit
	d.randomLatencyMin = min
	d.randomLatencyMax = max
	return d
}

func (d *testDB) Get(ctx context.Context, key string, opt types.KVReadOption) (types.Value, error) {
	if d.randomLatency {
		time.Sleep(utils.RandomPeriod(d.randomLatencyUnit, d.randomLatencyMin, d.randomLatencyMax))
	} else {
		time.Sleep(d.latency)
	}
	return d.KV.Get(ctx, key, opt)
}

func (d *testDB) Set(ctx context.Context, key string, val types.Value, opt types.KVWriteOption) error {
	if d.randomLatency {
		time.Sleep(utils.RandomPeriod(d.randomLatencyUnit, d.randomLatencyMin, d.randomLatencyMax))
	} else {
		time.Sleep(d.latency)
	}
	if d.failurePattern.IsReqLost() {
		if rand.Seed(time.Now().UnixNano()); rand.Intn(d.probabilityOfTxnRecordFailureDenominator) == 0 {
			return errors.ErrInject
		}
	}
	if err := d.KV.Set(ctx, key, val, opt); err != nil {
		return err
	}
	if d.failurePattern.IsAckLost() {
		if rand.Seed(time.Now().UnixNano()); rand.Intn(d.probabilityOfTxnRecordFailureDenominator) == 0 {
			return errors.ErrInject
		}
	}
	return nil
}

type ExecuteStatistic struct {
	SumRetryTimes int
	SumCost       time.Duration
	Count         int

	AverageRetryTimes float64
	AverageCost       time.Duration
	RetryDetails      types.RetryDetails
}

func NewExecuteStatistic() *ExecuteStatistic {
	return &ExecuteStatistic{RetryDetails: make(types.RetryDetails)}
}

func (es *ExecuteStatistic) collect(info ExecuteInfo) {
	es.SumRetryTimes += info.RetryTimes
	es.SumCost += info.Cost
	es.Count++
	es.RetryDetails.Collect(info.RetryDetails)
}

func (es *ExecuteStatistic) calc() {
	es.AverageRetryTimes = float64(es.SumRetryTimes) / float64(es.Count)
	es.AverageCost = es.SumCost / time.Duration(es.Count)
}

type ExecuteStatistics struct {
	total      ExecuteStatistic
	perTxnKind map[types.TxnKind]*ExecuteStatistic
}

func NewExecuteStatistics() *ExecuteStatistics {
	return &ExecuteStatistics{
		total:      *NewExecuteStatistic(),
		perTxnKind: make(map[types.TxnKind]*ExecuteStatistic),
	}
}

func (ess *ExecuteStatistics) collect(info ExecuteInfo) {
	ess.total.collect(info)

	if ess.perTxnKind[info.Kind] == nil {
		ess.perTxnKind[info.Kind] = NewExecuteStatistic()
	}
	ess.perTxnKind[info.Kind].collect(info)
}

func (ess *ExecuteStatistics) calc() ExecuteStatistics {
	ess.total.calc()
	for _, es := range ess.perTxnKind {
		es.calc()
	}
	return *ess
}

func (ess *ExecuteStatistics) ForEachTxnKind(f func(types.TxnKind, ExecuteStatistic)) {
	var kinds = make([]int, 0, len(ess.perTxnKind))
	for kind := range ess.perTxnKind {
		kinds = append(kinds, int(kind))
	}
	sort.Ints(kinds)
	for _, kind := range kinds {
		k := types.TxnKind(kind)
		f(k, *ess.perTxnKind[k])
	}
}

type ExecuteInfo struct {
	types.Txn

	ID          uint64
	Kind        types.TxnKind
	ReadValues  map[string]types.TValue
	WriteValues map[string]types.Value

	RetryTimes   int
	RetryDetails types.RetryDetails
	Cost         time.Duration

	AdditionalInfo interface{}
}

func NewExecuteInfo(tx types.Txn, retryTimes int, retryDetails types.RetryDetails, cost time.Duration) ExecuteInfo {
	txn := ExecuteInfo{
		Txn:          tx,
		ID:           tx.GetId().Version(),
		ReadValues:   tx.GetReadValues(),
		WriteValues:  tx.GetWriteValues(),
		RetryTimes:   retryTimes,
		RetryDetails: retryDetails,
		Cost:         cost,
	}
	txn.Kind = types.TxnKindReadWrite
	if len(txn.WriteValues) == 0 {
		txn.Kind = types.TxnKindReadOnly
	} else if len(txn.ReadValues) == 0 {
		txn.Kind = types.TxnKindWriteOnly
	}
	return txn
}

func (info ExecuteInfo) WithAdditionalInfo(object interface{}) ExecuteInfo {
	info.AdditionalInfo = object
	return info
}

func (info ExecuteInfo) SortIndex() uint64 {
	if info.GetType().IsSnapshotRead() {
		return info.GetSnapshotReadOption().SnapshotVersion
	}
	return info.ID
}

func (info ExecuteInfo) String() string {
	return fmt.Sprintf("Txn{id: %d, state: %s, read: %v, write: %v}", info.ID, info.GetState().String(), info.ReadValues, info.WriteValues)
}

type ExecuteInfos []ExecuteInfo

func (txns ExecuteInfos) Len() int {
	return len(txns)
}
func (txns ExecuteInfos) Less(i, j int) bool {
	txnI, txnJ := txns[i], txns[j]
	indexI, indexJ := txnI.SortIndex(), txnJ.SortIndex()
	if indexI < indexJ {
		return true
	}
	if indexI > indexJ {
		return false
	}
	return !txnI.GetType().IsSnapshotRead()
}
func (txns ExecuteInfos) Swap(i, j int) {
	txns[i], txns[j] = txns[j], txns[i]
}

func (txns ExecuteInfos) CheckSerializability(assert *types.Assertions) bool {
	sort.Sort(txns)

	lastWriteTxns := make(map[string]int)
	for i := 0; i < len(txns); i++ {
		var (
			txn = txns[i]
		)
		if opt := txn.GetSnapshotReadOption(); txn.GetType().IsSnapshotRead() {
			var ssVersion = txn.GetSnapshotReadOption().SnapshotVersion
			if !assert.NotEmpty(ssVersion) {
				return false
			}
			if !assert.GreaterOrEqual(ssVersion, opt.MinAllowedSnapshotVersion) {
				return false
			}
			for _, readVal := range txn.ReadValues {
				if !opt.AllowsVersionBack() {
					if !assert.Equal(readVal.SnapshotVersion, ssVersion) {
						return false
					}
				} else {
					if !assert.GreaterOrEqual(readVal.SnapshotVersion, ssVersion) {
						return false
					}
				}
				if !assert.GreaterOrEqual(opt.MinAllowedSnapshotVersion, readVal.Version) {
					return false
				}
			}
		}
		for key, readVal := range txn.ReadValues {
			if readVal.Version == txn.ID {
				if !assert.Contains(txn.WriteValues, key) {
					return false
				}
			} else if lastWriteTxnIndex := lastWriteTxns[key]; lastWriteTxnIndex != 0 {
				lastWriteTxn := txns[lastWriteTxnIndex]
				if !assert.EqualValue(lastWriteTxn.WriteValues[key], readVal.Value) {
					return false
				}
			}
		}

		for key := range txns[i].WriteValues {
			lastWriteTxns[key] = i
		}
	}
	return true
}

//func (txns ExecuteInfos) findLastWriteVersion(searchStart int, key string) types.Value {
//	for j := searchStart; j >= 0; j-- {
//		if writeVal, ok := txns[j].WriteValues[key]; ok {
//			return writeVal
//		}
//	}
//	return types.EmptyValue
//}

func (txns ExecuteInfos) Statistics() ExecuteStatistics {
	ess := NewExecuteStatistics()
	for _, txn := range txns {
		ess.collect(txn)
	}
	return ess.calc()
}

type TestCase struct {
	t types.T
	*types.Assertions

	embeddedTablet bool

	rounds   int
	testFunc func(context.Context, *TestCase) bool

	DBType                   types.DBType
	TxnType, ReadOnlyTxnType types.TxnType

	TxnManagerCfg types.TxnManagerConfig
	TableTxnCfg   types.TabletTxnConfig

	GoRoutineNum                     int
	TxnNumPerGoRoutine               int
	timeoutPerRound                  time.Duration
	LogLevel                         glog.Level
	maxRetryPerTxn                   int
	snapshotReadDontAllowVersionBack bool

	SimulatedLatency                   time.Duration
	RandomLatency                      bool
	RandomLatencyUnit                  time.Duration
	RandomLatencyMin, RandomLatencyMax int
	FailurePattern                     FailurePattern
	FailureProbability                 int

	additionalParameters map[string]interface{}

	// output
	scs         []*smart_txn_client.SmartClient // can be set too, if setted, will ignore gen test env
	txnServers  []*Server
	txnManagers []types.TxnManager
	stopper     func()

	// statistics
	executedTxnsPerGoRoutine     [][]ExecuteInfo
	skipRoundsCheck, extraRounds []bool
	allExecutedTxns              ExecuteInfos
}

const (
	defaultTimeoutPerRound = time.Minute
)

func NewTestCase(t types.T, rounds int, testFunc func(context.Context, *TestCase) bool) *TestCase {
	return (&TestCase{
		t:          t,
		rounds:     rounds,
		Assertions: types.NewAssertion(t),
		testFunc:   testFunc,

		LogLevel:           glog.Level(5),
		GoRoutineNum:       6,
		TxnNumPerGoRoutine: 1000,
		DBType:             types.DBTypeMemory,
		TxnType:            types.TxnTypeDefault,
		ReadOnlyTxnType:    types.TxnTypeDefault,

		TxnManagerCfg: defaultTxnManagerConfig,
		TableTxnCfg:   defaultTabletTxnConfig,

		timeoutPerRound: defaultTimeoutPerRound,
	}).initialGoRoutineRelatedFields()
}

func NewEmbeddedTestCase(t types.T, rounds int, testFunc func(context.Context, *TestCase) bool) *TestCase {
	return NewTestCase(t, rounds, testFunc).SetGoRoutineNum(10000).SetTxnNumPerGoRoutine(1).SetEmbeddedTablet()
}

func NewEmbeddedSnapshotReadTestCase(t types.T, rounds int, testFunc func(context.Context, *TestCase) bool) *TestCase {
	return NewEmbeddedTestCase(t, rounds, testFunc).SetReadOnlyTxnType(types.TxnTypeSnapshotRead)
}

func (ts *TestCase) Run() {
	ts.NoError(utils.MkdirIfNotExists(consts.DefaultTestLogDir))
	ts.NoError(flag.Set("log_dir", consts.DefaultTestLogDir))
	ts.NoError(flag.Set("alsologtostderr", fmt.Sprintf("%t", true)))
	ts.NoError(flag.Set("v", fmt.Sprintf("%d", ts.LogLevel)))
	ts.LogParameters()

	for i := 0; i < ts.rounds; i++ {
		if !ts.runOneRound(i) {
			ts.t.Errorf("%s failed @round %d", ts.t.Name(), i)
			return
		}
	}
}

func (ts *TestCase) runOneRound(i int) bool {
	ts.t.Logf("round %d", i)
	ts.allExecutedTxns = nil
	ts.initialGoRoutineRelatedFields()

	ctx, cancel := context.WithTimeout(context.Background(), ts.timeoutPerRound)
	defer cancel()

	if genResult := ts.GenTestEnv(); !ts.True(genResult) {
		return false
	}
	defer ts.Close()

	start := time.Now()
	if !ts.True(ts.testFunc(ctx, ts)) {
		return false
	}
	cost := time.Since(start)
	if !ts.CheckSerializability() {
		return false
	}
	ts.LogExecuteInfos(float64(cost) / float64(time.Second))
	return true
}

func (ts *TestCase) DoTransaction(ctx context.Context, goRoutineIndex int, sc *smart_txn_client.SmartClient,
	f func(ctx context.Context, txn types.Txn) error) bool {
	return ts.DoTransactionOfOption(ctx, goRoutineIndex, sc, types.NewTxnOption(ts.TxnType), f)
}
func (ts *TestCase) DoReadOnlyTransaction(ctx context.Context, goRoutineIndex int, sc *smart_txn_client.SmartClient,
	f func(ctx context.Context, txn types.Txn) error) bool {
	return ts.DoTransactionOfOption(ctx, goRoutineIndex, sc, types.NewTxnOption(ts.ReadOnlyTxnType).CondSnapshotReadDontAllowVersionBack(ts.snapshotReadDontAllowVersionBack), f)
}
func (ts *TestCase) DoTransactionOfOption(ctx context.Context, goRoutineIndex int, sc *smart_txn_client.SmartClient, opt types.TxnOption,
	f func(ctx context.Context, txn types.Txn) error) bool {
	start := time.Now()
	if tx, retryTimes, retryDetails, err := sc.DoTransactionOfOption(ctx, opt, f); ts.NoError(err) {
		var totalCount int
		for _, count := range retryDetails {
			totalCount += count
		}
		assert.Must(retryTimes-1 == totalCount)
		ts.CollectExecutedTxnInfo(goRoutineIndex, tx, retryTimes, retryDetails, time.Since(start))
		return true
	}
	return false
}

func (ts *TestCase) GenTestEnv() bool {
	if len(ts.scs) > 0 {
		return true
	}

	if ts.embeddedTablet {
		titan := newDB(ts.Assertions, ts.DBType, 0)
		if !ts.NotNil(titan) {
			ts.Close()
			return false
		}
		kvc := kvcc.NewKVCCForTesting(newTestDB(titan, ts.SimulatedLatency, ts.FailurePattern, ts.FailureProbability).
			SetRandomLatency(ts.RandomLatency, ts.RandomLatencyUnit, ts.RandomLatencyMin, ts.RandomLatencyMax), ts.TableTxnCfg)
		m := NewTransactionManager(kvc, ts.TxnManagerCfg).SetRecordValuesTxn(true)
		ts.txnManagers = append(ts.txnManagers, m)
		ts.stopper = func() {
			ts.NoError(m.Close())
		}
		sc := smart_txn_client.NewSmartClient(m, ts.maxRetryPerTxn)
		ts.SetSmartClients(sc)
		return true
	}

	var clientTxnManagers []*ClientTxnManager
	if ts.txnServers, clientTxnManagers, ts.stopper = createCluster(ts.Assertions, ts.DBType, ts.TxnManagerCfg,
		ts.TableTxnCfg); !ts.Len(ts.txnServers, 2) {
		return false
	}
	for _, clientTm := range clientTxnManagers {
		ts.txnManagers = append(ts.txnManagers, clientTm)
	}
	ctm1, ctm2 := ts.txnManagers[0], ts.txnManagers[1]
	sc2 := smart_txn_client.NewSmartClient(ctm2, 10000)
	sc1 := smart_txn_client.NewSmartClient(ctm1, 10000)
	ts.scs = []*smart_txn_client.SmartClient{sc1, sc2}
	return true
}

func (ts *TestCase) CollectExecutedTxnInfo(goRoutineIndex int, tx types.Txn, retryTimes int, retryDetails types.RetryDetails, cost time.Duration) {
	txn := NewExecuteInfo(tx, retryTimes, retryDetails, cost)
	ts.False(txn.ID == 0)
	ts.True(txn.GetState() == types.TxnStateCommitted)
	ts.executedTxnsPerGoRoutine[goRoutineIndex] = append(ts.executedTxnsPerGoRoutine[goRoutineIndex], txn)
}

func (ts *TestCase) CheckSerializability() bool {
	if len(ts.allExecutedTxns) > 0 {
		// already checked by user
		return true
	}
	if !ts.Len(ts.executedTxnsPerGoRoutine, ts.GoRoutineNum) {
		return false
	}
	ts.allExecutedTxns = make(ExecuteInfos, 0, len(ts.executedTxnsPerGoRoutine)*ts.TxnNumPerGoRoutine)
	for idx, txnsOneGoRoutine := range ts.executedTxnsPerGoRoutine {
		if ts.skipRoundsCheck[idx] {
			// pass
		} else if !ts.extraRounds[idx] {
			if !ts.Len(txnsOneGoRoutine, ts.TxnNumPerGoRoutine) {
				return false
			}
		} else if !ts.Greater(len(txnsOneGoRoutine), ts.TxnNumPerGoRoutine) {
			return false
		}
		ts.allExecutedTxns = append(ts.allExecutedTxns, txnsOneGoRoutine...)
	}
	b := ts.allExecutedTxns.CheckSerializability(ts.Assertions)
	if b {
		ts.t.Logf("%s CheckSerializability passed", ts.t.Name())
	}
	return b
}

// CheckReadModifyWriteOnly only used for checking only have update (read for write) transactions in the test case
func (ts *TestCase) CheckReadModifyWriteOnly(keys ...string) bool {
	if !ts.NotEmpty(keys, "must check something for CheckReadModifyWriteOnly") {
		return false
	}
	sort.Sort(ts.allExecutedTxns)
	for i := 1; i < len(ts.allExecutedTxns); i++ {
		prev, cur := ts.allExecutedTxns[i-1], ts.allExecutedTxns[i]
		for _, key := range keys {
			if prevWrite, curRead := prev.WriteValues[key], cur.ReadValues[key]; !ts.EqualIntValue(prevWrite, curRead.Value) {
				return false
			}
		}
	}
	return true
}

func (ts *TestCase) SetEmbeddedTablet() *TestCase           { ts.embeddedTablet = true; return ts }
func (ts *TestCase) GetGate1() *gate.Gate                   { return ts.txnServers[0].tm.kv.(*gate.Gate) }
func (ts *TestCase) GetGate2() *gate.Gate                   { return ts.txnServers[1].tm.kv.(*gate.Gate) }
func (ts *TestCase) SetTxnType(typ types.TxnType) *TestCase { ts.TxnType = typ; return ts }
func (ts *TestCase) AddTxnType(typ types.TxnType) *TestCase { ts.TxnType |= typ; return ts }
func (ts *TestCase) SetReadOnlyTxnType(typ types.TxnType) *TestCase {
	ts.ReadOnlyTxnType = typ
	return ts
}
func (ts *TestCase) AddReadOnlyTxnType(typ types.TxnType) *TestCase {
	ts.ReadOnlyTxnType |= typ
	return ts
}
func (ts *TestCase) SetSnapshotReadDontAllowVersionBack(b bool) *TestCase {
	ts.snapshotReadDontAllowVersionBack = b
	return ts
}
func (ts *TestCase) SetMaxRetryPerTxn(v int) *TestCase    { ts.maxRetryPerTxn = v; return ts }
func (ts *TestCase) SetDBType(typ types.DBType) *TestCase { ts.DBType = typ; return ts }
func (ts *TestCase) SetTimeoutPerRound(timeout time.Duration) *TestCase {
	ts.timeoutPerRound = timeout
	return ts
}
func (ts *TestCase) SetExtraRound(goRoutineIndex int) *TestCase {
	ts.extraRounds[goRoutineIndex] = true
	return ts
}
func (ts *TestCase) SetSkipRoundCheck(goRoutineIndex int) *TestCase {
	ts.skipRoundsCheck[goRoutineIndex] = true
	return ts
}
func (ts *TestCase) SetStaleWriteThreshold(thr time.Duration) *TestCase {
	ts.TxnManagerCfg = defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(thr)
	ts.TableTxnCfg = defaultTabletTxnConfig.WithStaleWriteThreshold(thr)
	return ts
}
func (ts *TestCase) SetSmartClients(scs ...*smart_txn_client.SmartClient) *TestCase {
	ts.scs = scs
	return ts
}
func (ts *TestCase) SetAdditionalParameters(key string, obj interface{}) *TestCase {
	if ts.additionalParameters == nil {
		ts.additionalParameters = make(map[string]interface{})
	}
	ts.additionalParameters[key] = obj
	return ts
}
func (ts *TestCase) SetGoRoutineNum(num int) *TestCase {
	ts.GoRoutineNum = num
	return ts.initialGoRoutineRelatedFields()
}
func (ts *TestCase) SetTxnNumPerGoRoutine(num int) *TestCase { ts.TxnNumPerGoRoutine = num; return ts }
func (ts *TestCase) SetLogLevel(lvl int) *TestCase           { ts.LogLevel = glog.Level(lvl); return ts }
func (ts *TestCase) SetSimulatedLatency(latency time.Duration) *TestCase {
	ts.SimulatedLatency = latency
	return ts
}
func (ts *TestCase) SetRandomLatency(unit time.Duration, min, max int) *TestCase {
	ts.RandomLatency = true
	ts.RandomLatencyUnit = unit
	ts.RandomLatencyMin = min
	ts.RandomLatencyMax = max
	return ts
}
func (ts *TestCase) SetFailureProbability(prob int) *TestCase     { ts.FailureProbability = prob; return ts }
func (ts *TestCase) SetFailurePattern(p FailurePattern) *TestCase { ts.FailurePattern = p; return ts }

func (ts *TestCase) Close() {
	if ts.stopper != nil {
		ts.stopper()
	}

	for _, sc := range ts.scs {
		ts.NoError(sc.Close())
	}
	ts.scs = nil
	ts.txnServers = nil
	ts.txnManagers = nil
	ts.stopper = nil
}

func (ts *TestCase) LogParameters() {
	ts.t.Logf("%s\n"+
		"db type: \"%s\", txn type: \"%s\", read only txn type: \"%s\"\n"+
		"stale threshold: %s, wound threshold: %s\n"+
		"go routine number: %d, txn number per go routine: %d, log level: %d",
		ts.t.Name(),
		ts.DBType, ts.TxnType, ts.ReadOnlyTxnType,
		ts.TableTxnCfg.StaleWriteThreshold, ts.TxnManagerCfg.WoundUncommittedTxnThreshold,
		ts.GoRoutineNum, ts.TxnNumPerGoRoutine, ts.LogLevel)
}

func (ts *TestCase) LogExecuteInfos(totalCostInSeconds float64) {
	stats := ts.allExecutedTxns.Statistics()
	ts.t.Logf("Executed %d txns in %.1fs, throughput: %.1ftxn/s, average latency: %s, average retry: %.3f times",
		len(ts.allExecutedTxns), totalCostInSeconds, float64(len(ts.allExecutedTxns))/totalCostInSeconds, stats.total.AverageCost, stats.total.AverageRetryTimes)
	stats.ForEachTxnKind(func(txnKind types.TxnKind, ss ExecuteStatistic) {
		ts.t.Logf("    [%s] count: %d, average latency: %s, average retry: %.3f", txnKind, ss.Count, ss.AverageCost, ss.AverageRetryTimes)
		ts.t.Logf("      retry reasons: ")
		for _, item := range ss.RetryDetails.GetSortedRetryDetails() {
			ts.t.Logf("        " + item.String())
		}
	})
}

func (ts *TestCase) MustRouteToDifferentShards(key1 string, key2 string) bool {
	gAte1, gAte2 := ts.GetGate1(), ts.GetGate2()
	if !ts.NotNil(gAte1) ||
		!ts.NotNil(gAte2) {
		return false
	}
	return ts.True(gAte1.MustRoute(types.TxnKeyUnion{Key: key1}).ID != gAte1.MustRoute(types.TxnKeyUnion{Key: key2}).ID) &&
		ts.True(gAte2.MustRoute(types.TxnKeyUnion{Key: key1}).ID != gAte2.MustRoute(types.TxnKeyUnion{Key: key2}).ID)
}

func (ts *TestCase) initialGoRoutineRelatedFields() *TestCase {
	ts.executedTxnsPerGoRoutine = make([][]ExecuteInfo, ts.GoRoutineNum)
	ts.skipRoundsCheck = make([]bool, ts.GoRoutineNum)
	ts.extraRounds = make([]bool, ts.GoRoutineNum)
	return ts
}

func createCluster(assert *types.Assertions, dbType types.DBType, cfg types.TxnManagerConfig, tabletCfg types.TabletTxnConfig) (txnServers []*Server, clientTxnManagers []*ClientTxnManager, _ func()) {
	gates, stopTablets, stopGates := createTabletsGates(assert, dbType, tabletCfg)
	if !assert.Len(gates, 2) || !assert.NotNil(stopTablets) || !assert.NotNil(stopGates) {
		return nil, nil, nil
	}
	stop := stopTablets
	defer func() {
		if len(txnServers) == 0 {
			stop()
		}
	}()

	{
		const OracleServerPort = 6666
		// create oracle server
		oracleServer := createOracleServer(assert, OracleServerPort, dbType)
		if !assert.NotNil(oracleServer) {
			return nil, nil, nil
		}
		if err := oracleServer.Start(); !assert.NoError(err) {
			return nil, nil, nil
		}
		stopTablets := stop
		stop = func() {
			_ = oracleServer.Close()
			stopTablets()
		}
	}

	{
		// Create txn server 1
		const txnServer1Port = 16666
		cli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
		if !assert.NoError(err) {
			return nil, nil, nil
		}

		txnServer1, err := NewServer(txnServer1Port, gates[0], cfg, topo.NewStore(cli, defaultClusterName))
		if !assert.NoError(err) {
			return nil, nil, nil
		}
		if !assert.NoError(txnServer1.Start()) {
			return nil, nil, nil
		}
		txnServers = append(txnServers, txnServer1)
		stopOracleTablets := stop
		stop = func() {
			assert.NoError(txnServer1.Close())
			stopOracleTablets()
		}
		tmCli1, err := NewClient(fmt.Sprintf("localhost:%d", txnServer1Port))
		if !assert.NoError(err) {
			return nil, nil, nil
		}
		clientTxnManagers = append(clientTxnManagers, NewClientTxnManager(tmCli1).SetRecordValues(true))
	}

	{
		// Create txn server 2
		const txnServer2Port = 17777
		cli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
		if !assert.NoError(err) {
			return nil, nil, nil
		}

		txnServer2, err := NewServer(txnServer2Port, gates[1], cfg, topo.NewStore(cli, defaultClusterName))
		if !assert.NoError(err) {
			return nil, nil, nil
		}
		if !assert.NoError(txnServer2.Start()) {
			return nil, nil, nil
		}
		txnServers = append(txnServers, txnServer2)
		stopOracleTablets := stop
		stop = func() {
			assert.NoError(txnServer2.Close())
			stopOracleTablets()
		}
		tmCli2, err := NewClient(fmt.Sprintf("localhost:%d", txnServer2Port))
		if !assert.NoError(err) {
			return nil, nil, nil
		}
		clientTxnManagers = append(clientTxnManagers, NewClientTxnManager(tmCli2).SetRecordValues(true))
	}
	return txnServers, clientTxnManagers, stop
}

func createTabletsGates(assert *types.Assertions, dbType types.DBType, tabletCfg types.TabletTxnConfig) (gates []*gate.Gate, stopTablets func(), stopGates func()) {
	if !assert.NoError(utils.RemoveDirIfExists("/tmp/data/")) {
		return nil, nil, nil
	}

	const (
		tablet1Port = 20000
		tablet2Port = 30000
	)
	stopTablets = func() {}
	defer func() {
		if len(gates) == 0 {
			stopTablets()
			stopTablets = nil
		}
	}()
	tablet1 := createTabletServer(assert, tablet1Port, dbType, 0, tabletCfg)
	if !assert.NotNil(tablet1) {
		return
	}
	if !assert.NoError(tablet1.Start()) {
		return
	}
	stopTablets = func() {
		assert.NoError(tablet1.Close())
	}
	tablet2 := createTabletServer(assert, tablet2Port, dbType, 1, tabletCfg)
	if !assert.NotNil(tablet2) {
		return
	}
	if !assert.NoError(tablet2.Start()) {
		return
	}
	oldStopper := stopTablets
	stopTablets = func() {
		assert.NoError(tablet2.Close())
		oldStopper()
	}

	stopGates = func() {}
	defer func() {
		if len(gates) == 0 {
			stopGates()
			stopGates = nil
		}
	}()
	{

		topoCLi, err := client.NewClient("fs", "/tmp/", "", time.Minute)
		if !assert.NoError(err) {
			return
		}
		g1, err := gate.NewGate(topo.NewStore(topoCLi, defaultClusterName))
		if !assert.NoError(err) {
			return
		}
		gates = append(gates, g1)
		stopGates = func() {
			assert.NoError(g1.Close())
		}
	}

	{

		topoCli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
		if !assert.NoError(err) {
			return
		}
		g2, err := gate.NewGate(topo.NewStore(topoCli, defaultClusterName))
		if !assert.NoError(err) {
			return
		}
		gates = append(gates, g2)
		stopGate1 := stopGates
		stopGates = func() {
			assert.NoError(g2.Close())
			stopGate1()
		}
	}
	return gates, stopTablets, stopGates
}

func createGate(assert *types.Assertions, tabletCfg types.TabletTxnConfig) (g *gate.Gate, _ func()) {
	return createGateEx(assert, types.DBTypeMemory, tabletCfg)
}

func createGateEx(assert *types.Assertions, dbType types.DBType, tabletCfg types.TabletTxnConfig) (g *gate.Gate, _ func()) {
	if !assert.NoError(utils.RemoveDirIfExists("/tmp/data/")) {
		return nil, nil
	}

	const (
		tablet1Port = 20000
		tablet2Port = 30000
	)
	stopper := func() {}
	defer func() {
		if g == nil {
			stopper()
		}
	}()
	tablet1 := createTabletServer(assert, tablet1Port, dbType, 0, tabletCfg)
	if !assert.NotNil(tablet1) {
		return nil, nil
	}
	if !assert.NoError(tablet1.Start()) {
		return nil, nil
	}
	stopper = func() {
		_ = tablet1.Close()
	}
	tablet2 := createTabletServer(assert, tablet2Port, dbType, 1, tabletCfg)
	if !assert.NotNil(tablet2) {
		return nil, nil
	}
	if !assert.NoError(tablet2.Start()) {
		return nil, nil
	}
	oldStopper := stopper
	stopper = func() {
		oldStopper()
		_ = tablet2.Close()
	}

	cli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
	if !assert.NoError(err) {
		return nil, nil
	}
	if g, err = gate.NewGate(topo.NewStore(cli, defaultClusterName)); !assert.NoError(err) {
		return nil, nil
	}
	return g, stopper
}

func createTabletServer(assert *types.Assertions, port int, dbType types.DBType, gid int, cfg types.TabletTxnConfig) (server *kvcc.Server) {
	cli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
	if !assert.NoError(err) {
		return nil
	}
	db := newDB(assert, dbType, gid)
	if !assert.NotNil(db) {
		return nil
	}
	return kvcc.NewServerForTesting(port, db, cfg, gid, topo.NewStore(cli, defaultClusterName))
}

func createOracleServer(assert *types.Assertions, port int, dbType types.DBType) *impl.Server {
	oracleTopoCli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
	if !assert.NoError(err) {
		return nil
	}
	var ora oracle.Oracle
	switch dbType {
	case types.DBTypeRedis:
		ora = physical.NewLoosedPrecisionOracle()
	case types.DBTypeMemory, types.DBTypeMongo:
		ora = physical.NewOracle()
	default:
		assert.Failf("newDB failed", "unsupported db type %s", dbType)
		return nil
	}
	return impl.NewServer(port, ora, topo.NewStore(oracleTopoCli, defaultClusterName))
}

var gid2RedisPort = map[int]int{
	0: 6379,
	1: 16379,
	2: 26379,
}

var gid2MongoPort = map[int]int{
	0: 27017,
	1: 37037,
	2: 47047,
	3: 57057,
}

func newDB(assert *types.Assertions, dbType types.DBType, gid int) types.KV {
	switch dbType {
	case types.DBTypeMongo:
		port, ok := gid2MongoPort[gid]
		if !ok {
			panic(fmt.Sprintf("gid(%d) too big", gid))
		}
		db, err := mongodb.NewDB(fmt.Sprintf("127.0.0.1:%d", port), nil)
		if !assert.NoError(err) {
			return nil
		}
		return db
	case types.DBTypeRedis:
		port, ok := gid2RedisPort[gid]
		if !ok {
			panic(fmt.Sprintf("gid(%d) too big", gid))
		}
		cli, err := redis.NewDB(fmt.Sprintf("127.0.0.1:%d", port), "")
		if !assert.NoError(err) {
			return nil
		}
		return cli
	case types.DBTypeMemory:
		return memory.NewMemoryDB()
	default:
		assert.Failf("newDB failed", "unsupported db type %s", dbType)
		return nil
	}
}
