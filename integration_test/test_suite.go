package integration

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"testing"

	"github.com/leisurelyrcxf/spermwhale/kv"
	"github.com/leisurelyrcxf/spermwhale/txn"

	"github.com/leisurelyrcxf/spermwhale/txn/smart_txn_client"

	testconsts "github.com/leisurelyrcxf/spermwhale/integration_test/consts"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/integration_test/types"
	"github.com/leisurelyrcxf/spermwhale/integration_test/utils"
	"github.com/leisurelyrcxf/spermwhale/sync2"
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
	exiting  sync2.AtomicBool
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

	txnClient, err := txn.NewClient(fmt.Sprintf("localhost:%d", txnPort))
	if !t.NoError(err) {
		return
	}
	t.TxnClient = smart_txn_client.NewSmartClient(txnClient)
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

type MyT struct {
	t *testing.T
	*TestSuite
}

func NewT(t *testing.T, ts *TestSuite) MyT {
	return MyT{
		t:         t,
		TestSuite: ts,
	}
}

func (t MyT) Errorf(format string, args ...interface{}) {
	if isMain() && !t.exiting.Get() {
		t.t.Errorf(format, args...)
		return
	}
	print(fmt.Sprintf(format, args...))
	_ = os.Stderr.Sync()
	t.Exit()
}

func isMain() bool {
	ss := string(debug.Stack())
	return strings.Contains(ss, "testing.(*T).Run")
}
