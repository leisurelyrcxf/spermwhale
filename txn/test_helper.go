package txn

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/txn/smart_txn_client"

	"github.com/leisurelyrcxf/spermwhale/kv/impl/mongodb"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/gate"
	"github.com/leisurelyrcxf/spermwhale/kv/impl/memory"
	"github.com/leisurelyrcxf/spermwhale/kv/impl/redis"
	"github.com/leisurelyrcxf/spermwhale/kvcc"
	"github.com/leisurelyrcxf/spermwhale/oracle"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"
	"github.com/leisurelyrcxf/spermwhale/topo"
	"github.com/leisurelyrcxf/spermwhale/topo/client"
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
	ID                      uint64
	State                   types.TxnState
	ReadValues, WriteValues map[string]types.Value
	AdditionalInfo          interface{}
}

func (info ExecuteInfo) String() string {
	return fmt.Sprintf("Txn{id: %d, state: %s, read: %v, write: %v}", info.ID, info.State.String(), info.ReadValues, info.WriteValues)
}

type ExecuteInfos []ExecuteInfo

func (ss ExecuteInfos) Len() int {
	return len(ss)
}
func (ss ExecuteInfos) Less(i, j int) bool {
	return ss[i].ID < ss[j].ID
}
func (ss ExecuteInfos) Swap(i, j int) {
	ss[i], ss[j] = ss[j], ss[i]
}

// CheckReadForWriteOnly only used for checking only have update (read for write) transactions in the test case
func (ss ExecuteInfos) CheckReadForWriteOnly(assert *testifyassert.Assertions, key string) bool {
	sort.Sort(ss)
	for i := 1; i < len(ss); i++ {
		prev, cur := ss[i-1], ss[i]
		if !assert.Equal(prev.ID, cur.ReadValues[key].Version) || !assert.Equal(prev.WriteValues[key].MustInt(), cur.ReadValues[key].MustInt()) {
			return false
		}
	}
	return true
}

func (ss ExecuteInfos) Check(assert *testifyassert.Assertions) bool {
	sort.Sort(ss)
	for i := 0; i < len(ss); i++ {
		findLastWriteVersion := func(cur int, key string, readVal types.Value) types.Value {
			for j := i - 1; j > 0; j-- {
				ptx := ss[j]
				if writeVal, ok := ptx.WriteValues[key]; ok {
					return writeVal
				}
			}
			return types.EmptyValue
		}
		for key, readVal := range ss[i].ReadValues {
			lastWriteVal := findLastWriteVersion(i, key, readVal)
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

type TestCase struct {
	t *testing.T
	*testifyassert.Assertions

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
}

func NewTestCase(t *testing.T) *TestCase {
	return &TestCase{
		t:          t,
		Assertions: types.NewAssertion(t),

		LogLevel:           glog.Level(7),
		GoRoutineNum:       6,
		TxnNumPerGoRoutine: 1000,
		DBType:             types.DBTypeMemory,
		TxnType:            types.TxnTypeDefault,
		ReadOpt:            types.NewTxnReadOption(),

		TxnManagerCfg: defaultTxnManagerConfig,
		TableTxnCfg:   defaultTabletTxnConfig,
	}
}

func (ts *TestCase) genIntegrateTestEnv() bool {
	if len(ts.scs) == 2 {
		return true
	}
	if ts.txnServers, ts.clientTMs, ts.stopper = createCluster(ts.t, ts.DBType, ts.TxnManagerCfg,
		ts.TableTxnCfg); !ts.Len(ts.txnServers, 2) {
		return false
	}
	ctm1, ctm2 := ts.clientTMs[0], ts.clientTMs[1]
	sc1 := smart_txn_client.NewSmartClient(ctm1, 10000)
	sc2 := smart_txn_client.NewSmartClient(ctm2, 10000)
	ts.scs = []*smart_txn_client.SmartClient{sc1, sc2}
	return true
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

func (ts *TestCase) SetStaleWriteThreshold(thr time.Duration) *TestCase {
	ts.TxnManagerCfg = defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(thr)
	ts.TableTxnCfg = defaultTabletTxnConfig.WithStaleWriteThreshold(thr)
	return ts
}

func (ts *TestCase) SetSmartClients(scs ...*smart_txn_client.SmartClient) *TestCase {
	ts.scs = scs
	return ts
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
		clientTxnManagers = append(clientTxnManagers, NewClientTxnManager(tmCli1))
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
		clientTxnManagers = append(clientTxnManagers, NewClientTxnManager(tmCli2))
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
