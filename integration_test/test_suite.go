package integration

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/leisurelyrcxf/spermwhale/types/basic"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	testconsts "github.com/leisurelyrcxf/spermwhale/integration_test/consts"
	"github.com/leisurelyrcxf/spermwhale/integration_test/types"
	"github.com/leisurelyrcxf/spermwhale/kv"
	"github.com/leisurelyrcxf/spermwhale/txn"
	"github.com/leisurelyrcxf/spermwhale/txn/smart_txn_client"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

var (
	errBinaryNotExistsInPath = &errors.Error{
		Code: consts.ErrCodeUnknown,
		Msg:  "binary not exists in path",
	}
)

var (
	testPrerequisiteOnce sync.Once
	chkPrerequisiteErr   error
)

const (
	spermBinaryGate   = "spgate"
	spermBinaryTablet = "sptablet"
)

type TestSuite struct {
	TxnClient *smart_txn_client.SmartClient
	KVClient  *kv.Client

	testifyassert.Assertions
	exiting  basic.AtomicBool
	commands []*types.Command
}

func NewTestSuite(t *testing.T) *TestSuite {
	assert := testifyassert.New(t)
	if err := chkTestPrerequisite(); !assert.NoError(err) {
		return nil
	}
	ts := &TestSuite{
		Assertions: *testifyassert.New(t),
	}
	b := ts.CreateCluster()
	if !assert.True(b) {
		ts.CloseWithResult(&b)
		return nil
	}
	return ts
}

func (t *TestSuite) CreateCluster() (b bool) {
	const (
		kvPort  = 9999
		txnPort = 10001
	)

	if !t.True(t.StartProcess("sptablet --", []int{30000}, "", nil)) {
		return
	}
	if !t.True(t.StartProcess("sptablet --", []int{40000}, "", nil)) {
		return
	}
	if !t.True(t.StartProcess("spgate --", []int{kvPort, txnPort}, "", nil)) {
		return
	}

	kvClient, err := kv.NewClient(fmt.Sprintf("localhost:%d", kvPort))
	if !t.NoError(err) {
		return
	}
	t.KVClient = kvClient

	cli, err := txn.NewClient(fmt.Sprintf("localhost:%d", txnPort))
	if !t.NoError(err) {
		return
	}
	txnClient := txn.NewClientTxnManager(cli)
	if !t.NoError(err) {
		return
	}
	t.TxnClient = smart_txn_client.NewSmartClient(txnClient, 0)
	return true
}

func (t *TestSuite) StartProcess(cmdString string, ports []int, desc string, onStop func()) bool {
	if onStop == nil {
		onStop = func() {}
	}
	cmd := types.NewCommand(cmdString, ports, testconsts.DefaultWorkDir, desc)
	err := cmd.StartProcess(onStop)
	if !t.NoError(err) {
		return false
	}

	t.commands = append(t.commands, cmd)
	return true
}

func (t *TestSuite) Exit() {
	t.exiting.Set(true)

	go func() {
		t.CloseWithResult(utils.NewBool(true))
		os.Exit(1)
	}()
}

func (t *TestSuite) CloseWithResult(res *bool) {
	var err error
	if *res {
		for _, cmd := range t.commands {
			err = errors.Wrap(err, cmd.Kill())
		}
	}
	err = errors.Wrap(err, t.TxnClient.Close())
	t.NoError(err)
}

var binaries = []string{spermBinaryGate, spermBinaryTablet}

func chkTestPrerequisite() error {
	testPrerequisiteOnce.Do(func() {
		for _, binary := range binaries {
			if !utils.BinaryExistsInPath(binary) {
				chkPrerequisiteErr = errors.Annotatef(errBinaryNotExistsInPath, binary)
				return
			}
		}
	})
	return chkPrerequisiteErr
}
