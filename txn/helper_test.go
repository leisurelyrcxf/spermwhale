package txn

import (
	"fmt"
	"testing"
	"time"

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
		_ = tablet1.Close()
	}
	tablet2 := createTabletServer(assert, tablet2Port, 1, cfg)
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

func createCluster(t *testing.T, cfg types.TxnConfig) (txnServers []*Server, clientTxnManagers []*ClientTxnManager, _ func()) {
	assert := testifyassert.New(t)

	gates, stopper := createGates(t, cfg)
	if !assert.Len(gates, 2) {
		return nil, nil, nil
	}
	defer func() {
		if len(txnServers) == 0 {
			stopper()
		}
	}()

	{
		// create oracle server
		cliOracle, err := client.NewClient("fs", "/tmp/", "", time.Minute)
		if !assert.NoError(err) {
			return nil, nil, nil
		}
		oracleServer := impl.NewServer(5555, physical.NewOracle(), topo.NewStore(cliOracle, "test_cluster"))
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
		cli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
		if !assert.NoError(err) {
			return nil, nil, nil
		}

		s1, err := NewServer(50000, g1, cfg, 10, 15, topo.NewStore(cli, "test_cluster"))
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
		tmCli1, err := NewClient(fmt.Sprintf("localhost:%d", 50000))
		if !assert.NoError(err) {
			return nil, nil, nil
		}
		clientTxnManagers = append(clientTxnManagers, NewClientTxnManager(tmCli1))
	}

	{
		cli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
		if !assert.NoError(err) {
			return nil, nil, nil
		}

		s2, err := NewServer(60000, g2, cfg, 20, 25, topo.NewStore(cli, "test_cluster"))
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
		tmCli2, err := NewClient(fmt.Sprintf("localhost:%d", 60000))
		if !assert.NoError(err) {
			return nil, nil, nil
		}
		clientTxnManagers = append(clientTxnManagers, NewClientTxnManager(tmCli2))
	}
	return txnServers, clientTxnManagers, stopper
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
		tablet1.Close()
	}
	tablet2 := createTabletServer(assert, tablet2Port, 1, cfg)
	if !assert.NoError(tablet2.Start()) {
		return nil, nil
	}
	oldStopper := stopper
	stopper = func() {
		oldStopper()
		tablet2.Close()
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
