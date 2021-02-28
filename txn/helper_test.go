package txn

import (
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/cmd"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"

	"github.com/leisurelyrcxf/spermwhale/tablet"

	"github.com/leisurelyrcxf/spermwhale/gate"
	"github.com/leisurelyrcxf/spermwhale/topo"
	"github.com/leisurelyrcxf/spermwhale/topo/client"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
	testifyassert "github.com/stretchr/testify/assert"
)

func createGate(t *testing.T, cfg types.TxnConfig) (g *gate.Gate, _ func()) {
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
	tablet1 := createTabletServer(assert, tablet1Port, 0, cfg)
	if !assert.NoError(tablet1.Start()) {
		return nil, nil
	}
	stopper = func() {
		tablet1.Stop()
	}
	tablet2 := createTabletServer(assert, tablet2Port, 1, cfg)
	if !assert.NoError(tablet2.Start()) {
		return nil, nil
	}
	oldStopper := stopper
	stopper = func() {
		oldStopper()
		tablet2.Stop()
	}

	cli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
	if !assert.NoError(err) {
		return nil, nil
	}
	if g, err = gate.NewGate(topo.NewStore(cli, "test_cluster")); !assert.NoError(err) {
		return nil, nil
	}
	return g, stopper
}

func createTabletServer(assert *testifyassert.Assertions, port, gid int, cfg types.TxnConfig) (server *tablet.Server) {
	cli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
	if !assert.NoError(err) {
		return nil
	}
	return tablet.NewServerForTesting(port, cfg, gid, topo.NewStore(cli, "test_cluster"))
}

func createCluster(t *testing.T, cfg types.TxnConfig) (txnManagers []*TransactionManager, _ func()) {
	assert := testifyassert.New(t)

	gates, stopper := createGates(t, cfg)
	if !assert.Len(gates, 2) {
		return nil, nil
	}
	defer func() {
		if len(txnManagers) == 0 {
			stopper()
		}
	}()

	{
		// create oracle server
		cliOracle, err := client.NewClient("fs", "/tmp/", "", time.Minute)
		if !assert.NoError(err) {
			return nil, nil
		}
		oracleServer := impl.NewServer(*cmd.FlagPort, physical.NewOracle(), topo.NewStore(cliOracle, "test_cluster"))
		if err := oracleServer.Start(); !assert.NoError(err) {
			return nil, nil
		}
		os := stopper
		stopper = func() {
			oracleServer.Stop()
			os()
		}
	}

	g1, g2 := gates[0], gates[1]
	{
		cli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
		if !assert.NoError(err) {
			return nil, nil
		}

		tm1, err := NewTransactionManagerWithCluster(g1, cfg, 20, 30, topo.NewStore(cli, "test_cluster"))
		if !assert.NoError(err) {
			return nil, nil
		}
		oos := stopper
		stopper = func() {
			_ = tm1.Close()
			oos()
		}
		txnManagers = append(txnManagers, tm1)
	}

	{
		cli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
		if !assert.NoError(err) {
			return nil, nil
		}
		tm2, err := NewTransactionManagerWithCluster(g2, cfg, 20, 30, topo.NewStore(cli, "test_cluster"))
		if !assert.NoError(err) {
			return nil, nil
		}
		ooos := stopper
		stopper = func() {
			_ = tm2.Close()
			ooos()
		}
		txnManagers = append(txnManagers, tm2)
	}
	return txnManagers, stopper
}

func createGates(t *testing.T, cfg types.TxnConfig) (gates []*gate.Gate, _ func()) {
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
	tablet1 := createTabletServer(assert, tablet1Port, 0, cfg)
	if !assert.NoError(tablet1.Start()) {
		return nil, nil
	}
	stopper = func() {
		tablet1.Stop()
	}
	tablet2 := createTabletServer(assert, tablet2Port, 1, cfg)
	if !assert.NoError(tablet2.Start()) {
		return nil, nil
	}
	oldStopper := stopper
	stopper = func() {
		oldStopper()
		tablet2.Stop()
	}

	cli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
	if !assert.NoError(err) {
		return nil, nil
	}
	var g1, g2 *gate.Gate
	if g1, err = gate.NewGate(topo.NewStore(cli, "test_cluster")); !assert.NoError(err) {
		return nil, nil
	}
	if g2, err = gate.NewGate(topo.NewStore(cli, "test_cluster")); !assert.NoError(err) {
		return nil, nil
	}
	return []*gate.Gate{g1, g2}, stopper
}
