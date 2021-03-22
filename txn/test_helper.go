package txn

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

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

type memoryDB struct {
	types.KV
	latency                                  time.Duration
	probabilityOfTxnRecordFailureDenominator int
	failurePattern                           FailurePattern
}

func newMemoryDB(latency time.Duration, failurePattern FailurePattern, failureProbability int) *memoryDB {
	return &memoryDB{
		KV:                                       memory.NewMemoryDB(),
		latency:                                  latency,
		probabilityOfTxnRecordFailureDenominator: failureProbability,
		failurePattern:                           failurePattern,
	}
}

func (d *memoryDB) Get(ctx context.Context, key string, opt types.KVReadOption) (types.Value, error) {
	time.Sleep(d.latency)
	return d.KV.Get(ctx, key, opt)
}

func (d *memoryDB) Set(ctx context.Context, key string, val types.Value, opt types.KVWriteOption) error {
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

func createCluster(t *testing.T, txnManagerCfg types.TxnManagerConfig, tabletCfg types.TabletTxnConfig) (txnServers []*Server, clientTxnManagers []*ClientTxnManager, _ func()) {
	return createClusterEx(t, types.DBTypeMemory, txnManagerCfg, tabletCfg)
}

func createClusterEx(t *testing.T, dbType types.DBType, cfg types.TxnManagerConfig, tabletCfg types.TabletTxnConfig) (txnServers []*Server, clientTxnManagers []*ClientTxnManager, _ func()) {
	assert := testifyassert.New(t)

	gates, stopper := createGates(t, dbType, tabletCfg)
	if !assert.Len(gates, 2) {
		return nil, nil, nil
	}
	defer func() {
		if len(txnServers) == 0 {
			stopper()
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
		os := stopper
		stopper = func() {
			_ = oracleServer.Close()
			os()
		}
	}

	g1, g2 := gates[0], gates[1]
	{
		const txnServer1Port = 50000
		cli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
		if !assert.NoError(err) {
			return nil, nil, nil
		}

		s1, err := NewServer(txnServer1Port, g1, cfg, topo.NewStore(cli, defaultClusterName))
		if !assert.NoError(err) {
			return nil, nil, nil
		}
		if !assert.NoError(s1.Start()) {
			return nil, nil, nil
		}
		txnServers = append(txnServers, s1)
		oos := stopper
		stopper = func() {
			_ = s1.Close()
			oos()
		}
		tmCli1, err := NewClient(fmt.Sprintf("localhost:%d", txnServer1Port))
		if !assert.NoError(err) {
			return nil, nil, nil
		}
		clientTxnManagers = append(clientTxnManagers, NewClientTxnManager(tmCli1))
	}

	{
		const txnServer2Port = 60000
		cli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
		if !assert.NoError(err) {
			return nil, nil, nil
		}

		s2, err := NewServer(txnServer2Port, g2, cfg, topo.NewStore(cli, defaultClusterName))
		if !assert.NoError(err) {
			return nil, nil, nil
		}
		if !assert.NoError(s2.Start()) {
			return nil, nil, nil
		}
		txnServers = append(txnServers, s2)
		oos := stopper
		stopper = func() {
			_ = s2.Close()
			oos()
		}
		tmCli2, err := NewClient(fmt.Sprintf("localhost:%d", txnServer2Port))
		if !assert.NoError(err) {
			return nil, nil, nil
		}
		clientTxnManagers = append(clientTxnManagers, NewClientTxnManager(tmCli2))
	}
	return txnServers, clientTxnManagers, stopper
}

func createGates(t *testing.T, dbType types.DBType, tabletCfg types.TabletTxnConfig) (gates []*gate.Gate, _ func()) {
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
		if len(gates) == 0 {
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
		assert.NoError(tablet1.Close())
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
		assert.NoError(tablet2.Close())
	}

	cli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
	if !assert.NoError(err) {
		return nil, nil
	}
	var g1, g2 *gate.Gate
	if g1, err = gate.NewGate(topo.NewStore(cli, defaultClusterName)); !assert.NoError(err) {
		return nil, nil
	}
	if g2, err = gate.NewGate(topo.NewStore(cli, defaultClusterName)); !assert.NoError(err) {
		return nil, nil
	}
	return []*gate.Gate{g1, g2}, stopper
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
	db := getDB(assert, dbType, gid)
	if !assert.NotNil(db) {
		return nil
	}
	return kvcc.NewServerForTesting(port, db, cfg, gid, topo.NewStore(cli, defaultClusterName))
}

var gid2RedisPort = map[int]int{
	0: 6379,
	1: 16379,
	2: 26379,
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
	case types.DBTypeMemory:
		ora = physical.NewOracle()
	default:
		assert.Failf("getDB failed", "unsupported db type %s", dbType)
		return nil
	}
	return impl.NewServer(port, ora, topo.NewStore(oracleTopoCli, defaultClusterName))
}

func getDB(assert *testifyassert.Assertions, dbType types.DBType, gid int) types.KV {
	switch dbType {
	case types.DBTypeRedis:
		port, ok := gid2RedisPort[gid]
		if !ok {
			panic(fmt.Sprintf("gid(%d) too big", gid))
		}
		cli, err := redis.NewClient(fmt.Sprintf("127.0.0.1:%d", port), "")
		if !assert.NoError(err) {
			return nil
		}
		return cli
	case types.DBTypeMemory:
		return memory.NewMemoryDB()
	default:
		assert.Failf("getDB failed", "unsupported db type %s", dbType)
		return nil
	}
}
