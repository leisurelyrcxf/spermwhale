package integration

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"testing"

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
	testifyassert.Assertions
	exiting  sync2.AtomicBool
	commands []*types.Command
}

func NewTestSuite(t *testing.T) *TestSuite {
	assert := testifyassert.New(t)
	if err := chkTestPrerequisite(); !assert.NoError(err) {
		return nil
	}
	return &TestSuite{
		Assertions: *testifyassert.New(t),
	}
}

func (t *TestSuite) CreateCluster() (b bool) {
	if !t.True(t.StartProcess("sptablet --", 30000, "", nil)) {
		return
	}
	if !t.True(t.StartProcess("sptablet --", 40000, "", nil)) {
		return
	}
	if !t.True(t.StartProcess("spgate --", 10001, "", nil)) {
		return
	}
	return true
}

func (t *TestSuite) StartProcess(cmdString string, port int, desc string, onStop func()) bool {
	if onStop == nil {
		onStop = func() {}
	}
	cmd := types.NewCommand(cmdString, port, testconsts.DefaultWorkDir, desc)
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
		t.close()
		os.Exit(1)
	}()
}

func (t *TestSuite) close() {

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
