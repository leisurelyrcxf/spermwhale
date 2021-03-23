package txn

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/golang/glog"

	testifyassert "github.com/stretchr/testify/assert"

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
	rounds             = 10
)

var (
	defaultTxnManagerConfig = types.NewTxnManagerConfig(time.Millisecond * 1000)
	defaultTabletTxnConfig  = types.NewTabletTxnConfig(time.Millisecond * 1000).WithMaxClockDrift(0)
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

func (d *testDB) Get(ctx context.Context, key string, opt types.KVReadOption) (types.Value, error) {
	time.Sleep(d.latency)
	return d.KV.Get(ctx, key, opt)
}

func (d *testDB) Set(ctx context.Context, key string, val types.Value, opt types.KVWriteOption) error {
	time.Sleep(d.latency)
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

type ExecuteInfo struct {
	types.Txn

	ID                      uint64
	State                   types.TxnState
	ReadValues, WriteValues map[string]types.Value
	RetryTimes              int
	Cost                    time.Duration
	AdditionalInfo          interface{}
}

func (info ExecuteInfo) String() string {
	return fmt.Sprintf("Txn{id: %d, state: %s, read: %v, write: %v}", info.ID, info.State.String(), info.ReadValues, info.WriteValues)
}

type ExecuteInfos []ExecuteInfo

func (txns ExecuteInfos) Len() int {
	return len(txns)
}
func (txns ExecuteInfos) Less(i, j int) bool {
	return txns[i].ID < txns[j].ID
}
func (txns ExecuteInfos) Swap(i, j int) {
	txns[i], txns[j] = txns[j], txns[i]
}

// CheckReadForWriteOnly only used for checking only have update (read for write) transactions in the test case
func (txns ExecuteInfos) CheckReadForWriteOnly(assert *testifyassert.Assertions, key string) bool {
	sort.Sort(txns)
	for i := 1; i < len(txns); i++ {
		prev, cur := txns[i-1], txns[i]
		if !assert.Equal(prev.ID, cur.ReadValues[key].Version) || !assert.Equal(prev.WriteValues[key].MustInt(), cur.ReadValues[key].MustInt()) {
			return false
		}
	}
	return true
}

func (txns ExecuteInfos) CheckSerializability(assert *testifyassert.Assertions) bool {
	sort.Sort(txns)

	findSnapshotTxnIndex := func(searchStart int, ssVersion uint64) int {
		for j := searchStart; j >= 0; j-- {
			if txns[j].ID <= ssVersion {
				return j
			}
		}
		return -1
	}

	findLastWriteVersion := func(searchStart int, key string, readVal types.Value) types.Value {
		for j := searchStart; j >= 0; j-- {
			ptx := txns[j]
			if writeVal, ok := ptx.WriteValues[key]; ok {
				return writeVal
			}
		}
		return types.EmptyValue
	}
	for i := 0; i < len(txns); i++ {
		for key, readVal := range txns[i].ReadValues {
			var lastWriteVal types.Value
			if txns[i].GetType().IsSnapshotRead() {
				ssVersion := txns[i].GetSnapshotVersion()
				if !assert.NotEqual(ssVersion, uint64(0)) || !assert.LessOrEqual(ssVersion, txns[i].ID) {
					return false
				}
				j := findSnapshotTxnIndex(i, ssVersion)
				if j == -1 {
					continue
				}
				lastWriteVal = findLastWriteVersion(j, key, readVal)
			} else {
				lastWriteVal = findLastWriteVersion(i-1, key, readVal)
			}
			if lastWriteVal.IsEmpty() {
				continue
			}
			if !assert.Equal(readVal.Version, lastWriteVal.Version) {
				return false
			}
		}
	}
	return true
}

func (txns ExecuteInfos) AverageRetryTimes() float64 {
	var totalRetryTimes int
	for _, info := range txns {
		totalRetryTimes += info.RetryTimes
	}
	return float64(totalRetryTimes) / float64(len(txns))
}

func (txns ExecuteInfos) AverageCost() time.Duration {
	var totalCost time.Duration
	for _, info := range txns {
		totalCost += info.Cost
	}
	return totalCost / time.Duration(len(txns))
}

type TestCase struct {
	t *testing.T
	*testifyassert.Assertions

	rounds   int
	testFunc func(context.Context, *TestCase) bool

	DBType  types.DBType
	TxnType types.TxnType
	ReadOpt types.TxnReadOption

	TxnManagerCfg types.TxnManagerConfig
	TableTxnCfg   types.TabletTxnConfig

	GoRoutineNum       int
	TxnNumPerGoRoutine int
	LogLevel           glog.Level

	//output, can be set too
	scs        []*smart_txn_client.SmartClient
	txnServers []*Server
	clientTMs  []*ClientTxnManager
	stopper    func()

	executedTxnsPerGoRoutine [][]ExecuteInfo
	timeoutPerRound          time.Duration
	extraRounds              []bool
	allExecutedTxns          ExecuteInfos
}

const (
	defaultGoRoutineNumber = 6
	defaultTimeoutPerRound = time.Minute
)

func NewTestCase(t *testing.T, rounds int, testFunc func(context.Context, *TestCase) bool) *TestCase {
	return &TestCase{
		t:          t,
		rounds:     rounds,
		Assertions: types.NewAssertion(t),
		testFunc:   testFunc,

		LogLevel:           glog.Level(5),
		GoRoutineNum:       defaultGoRoutineNumber,
		TxnNumPerGoRoutine: 1000,
		DBType:             types.DBTypeMemory,
		TxnType:            types.TxnTypeDefault,
		ReadOpt:            types.NewTxnReadOption(),

		TxnManagerCfg: defaultTxnManagerConfig,
		TableTxnCfg:   defaultTabletTxnConfig,

		timeoutPerRound: defaultTimeoutPerRound,
		extraRounds:     make([]bool, defaultGoRoutineNumber),
	}
}

func (ts *TestCase) Run() {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", ts.LogLevel))

	for i := 0; i < ts.rounds; i++ {
		if !ts.runOneRound(i) {
			ts.t.Errorf("%s failed @round %d", ts.t.Name(), i)
			return
		}
	}
}

func (ts *TestCase) runOneRound(i int) bool {
	ctx, cancel := context.WithTimeout(context.Background(), ts.timeoutPerRound)
	defer cancel()

	ts.LogParameters(i)
	if !ts.True(ts.GenTestEnv()) {
		return false
	}
	defer ts.Close()

	if !ts.True(ts.testFunc(ctx, ts)) || !ts.CheckSerializability() {
		return false
	}
	ts.LogTxnExecuteInfo()
	return true
}

func (ts *TestCase) DoTransaction(ctx context.Context, goRoutineIndex int, sc *smart_txn_client.SmartClient,
	f func(ctx context.Context, txn types.Txn) error) bool {
	start := time.Now()
	if tx, retryTimes, err := sc.DoTransactionOfTypeEx(ctx, ts.TxnType, f); ts.NoError(err) {
		ts.CollectExecutedTxnInfo(goRoutineIndex, tx, retryTimes, time.Since(start))
		return true
	}
	return false
}

func (ts *TestCase) GenTestEnv() bool {
	if len(ts.scs) == 2 {
		return true
	}
	if ts.txnServers, ts.clientTMs, ts.stopper = createCluster(ts.t, ts.DBType, ts.TxnManagerCfg,
		ts.TableTxnCfg); !ts.Len(ts.txnServers, 2) {
		return false
	}
	ctm1, ctm2 := ts.clientTMs[0], ts.clientTMs[1]
	sc2 := smart_txn_client.NewSmartClient(ctm2, 10000)
	sc1 := smart_txn_client.NewSmartClient(ctm1, 10000)
	ts.scs = []*smart_txn_client.SmartClient{sc1, sc2}
	ts.executedTxnsPerGoRoutine = make([][]ExecuteInfo, ts.GoRoutineNum)
	return true
}

func (ts *TestCase) CollectExecutedTxnInfo(goRoutineIndex int, tx types.Txn, retryTimes int, cost time.Duration) {
	txn := ExecuteInfo{
		Txn:         tx,
		ID:          tx.GetId().Version(),
		State:       tx.GetState(),
		ReadValues:  tx.GetReadValues(),
		WriteValues: tx.GetWriteValues(),
		RetryTimes:  retryTimes,
		Cost:        cost,
	}
	ts.False(txn.ID == 0)
	ts.True(txn.State == types.TxnStateCommitted)
	ts.False(types.IsInvalidReadValues(txn.ReadValues))
	ts.False(types.IsInvalidWriteValues(txn.WriteValues))
	ts.executedTxnsPerGoRoutine[goRoutineIndex] = append(ts.executedTxnsPerGoRoutine[goRoutineIndex], txn)
}

func (ts *TestCase) CheckSerializability() bool {
	if !ts.Len(ts.executedTxnsPerGoRoutine, ts.GoRoutineNum) {
		return false
	}
	ts.allExecutedTxns = make(ExecuteInfos, 0, len(ts.executedTxnsPerGoRoutine)*ts.TxnNumPerGoRoutine)
	for idx, txnsOneGoRoutine := range ts.executedTxnsPerGoRoutine {
		if !ts.extraRounds[idx] {
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

func (ts *TestCase) GetGate1() *gate.Gate {
	return ts.txnServers[0].tm.kv.(*gate.Gate)
}

func (ts *TestCase) GetGate2() *gate.Gate {
	return ts.txnServers[1].tm.kv.(*gate.Gate)
}

func (ts *TestCase) SetTxnType(typ types.TxnType) *TestCase {
	ts.TxnType = typ
	return ts
}

func (ts *TestCase) SetWaitNoWriteIntent() *TestCase {
	ts.ReadOpt = ts.ReadOpt.WithWaitNoWriteIntent()
	return ts
}

func (ts *TestCase) SetDBType(typ types.DBType) *TestCase {
	ts.DBType = typ
	return ts
}

func (ts *TestCase) SetTimeoutPerRound(timeout time.Duration) *TestCase {
	ts.timeoutPerRound = timeout
	return ts
}

func (ts *TestCase) SetExtraRound(goRoutineIndex int) *TestCase {
	ts.extraRounds[goRoutineIndex] = true
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

func (ts *TestCase) Close() {
	if ts.stopper != nil {
		ts.stopper()
	}

	for _, sc := range ts.scs {
		ts.NoError(sc.Close())
	}
	ts.scs = nil
	ts.txnServers = nil
	ts.clientTMs = nil
	ts.stopper = nil
}

func (ts *TestCase) LogParameters(round int) {
	ts.t.Logf("%s @round %d\n"+
		"db type: \"%s\", txn type: \"%s\", wait no write intent: %v\n"+
		"stale threshold: %s, wound threshold: %s\n"+
		"go routine number: %d, txn number per go routine: %d, log level: %d", ts.t.Name(), round,
		ts.DBType, ts.TxnType, ts.ReadOpt.IsWaitNoWriteIntent(),
		ts.TableTxnCfg.StaleWriteThreshold, ts.TxnManagerCfg.WoundUncommittedTxnThreshold,
		ts.GoRoutineNum, ts.TxnNumPerGoRoutine, ts.LogLevel)
}

func (ts *TestCase) LogTxnExecuteInfo() {
	ts.t.Logf("Excuted %d txns, average latency: %s, average retry: %.2f times", len(ts.allExecutedTxns), ts.allExecutedTxns.AverageCost(), ts.allExecutedTxns.AverageRetryTimes())
}

func (ts *TestCase) MustRouteToDifferentShards(key1 string, key2 string) bool {
	gAte1, gAte2 := ts.GetGate1(), ts.GetGate2()
	if !ts.NotNil(gAte1) ||
		!ts.NotNil(gAte2) {
		return false
	}
	return ts.True(gAte1.MustRoute(key1).ID != gAte1.MustRoute(key2).ID) &&
		ts.True(gAte2.MustRoute(key1).ID != gAte2.MustRoute(key2).ID)
}

func createCluster(t *testing.T, dbType types.DBType, cfg types.TxnManagerConfig, tabletCfg types.TabletTxnConfig) (txnServers []*Server, clientTxnManagers []*ClientTxnManager, _ func()) {
	assert := testifyassert.New(t)

	gates, stopTablets, stopGates := createTabletsGates(t, dbType, tabletCfg)
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

func createTabletsGates(t *testing.T, dbType types.DBType, tabletCfg types.TabletTxnConfig) (gates []*gate.Gate, stopTablets func(), stopGates func()) {
	assert := testifyassert.New(t)

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

func createGate(t types.T, tabletCfg types.TabletTxnConfig) (g *gate.Gate, _ func()) {
	return createGateEx(t, types.DBTypeMemory, tabletCfg)
}

func createGateEx(t types.T, dbType types.DBType, tabletCfg types.TabletTxnConfig) (g *gate.Gate, _ func()) {
	assert := testifyassert.New(t)

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

func createTabletServer(assert *testifyassert.Assertions, port int, dbType types.DBType, gid int, cfg types.TabletTxnConfig) (server *kvcc.Server) {
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

func createOracleServer(assert *testifyassert.Assertions, port int, dbType types.DBType) *impl.Server {
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

func newDB(assert *testifyassert.Assertions, dbType types.DBType, gid int) types.KV {
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
