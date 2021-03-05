package types

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/integration_test/consts"
	testutils "github.com/leisurelyrcxf/spermwhale/integration_test/utils"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

const (
	WatchAlivePeriod = 600 * time.Millisecond
)

type Status string

const (
	StatusInvalid       Status = ""
	StatusUninitialized Status = "uninitialized"
	StatusRunning       Status = "running"
	StatusStopped       Status = "Stopped"
)

type Command struct {
	cmd *exec.Cmd

	desc             string
	started, stopped chan struct{}
	WaitPorts        []int

	killed    atomic.Value
	startTime atomic.Value
	stopTime  atomic.Value

	mutex sync.Mutex
}

func NewCommand(cmdString string, ports []int, workDir, desc string) *Command {
	args := strings.Split(cmdString, "")
	cmd := exec.Command(args[0], args[1:]...) // nolint:gosec
	cmd.Dir = workDir
	return &Command{
		cmd:       cmd,
		desc:      desc,
		started:   make(chan struct{}),
		stopped:   make(chan struct{}),
		WaitPorts: ports,
	}
}

func (cmd *Command) StartProcess(onStop func()) (err error) {
	desc := cmd.desc
	if desc != "" {
		desc = fmt.Sprintf("(%s)", desc)
	}
	cmdString := cmd.FullName()
	cmdErrReader, err := cmd.cmd.StderrPipe()
	if err != nil {
		glog.Errorf("[StartProcess] can't get stderr pipe for cmd '%s'", cmdString)
		return err
	}

	errScanner := bufio.NewScanner(cmdErrReader)
	go func() {
		for errScanner.Scan() {
			glog.Infof("[%s] %s\n", cmd.cmd.Args[0], errScanner.Text())
		}
	}()

	if err := cmd.cmd.Start(); err != nil {
		return err
	}
	cmd.startTime.Store(testutils.Now())

	var cmdErr error

	go func() {
		if cmdErr = cmd.cmd.Wait(); cmdErr != nil {
			if !cmd.IsFake() || !testutils.IsKilled(cmdErr) {
				glog.Warningf("[StartProcess] command '%s'%s stopped with error '%v'", cmdString, desc, cmdErr)
			}
		} else {
			glog.Infof("[StartProcess] command '%s'%s stopped peacefully", cmdString, desc)
		}

		cmd.stopTime.Store(testutils.Now())
		onStop()
		cmd.NotifyStop()
	}()

	waitress := make(chan error, 1)
	if len(cmd.WaitPorts) == 0 || cmd.IsFake() {
		go func() {
			time.Sleep(WatchAlivePeriod)
			close(waitress)
		}()
	} else {
		go func() {
			if err := utils.WithContextRetryEx(context.Background(), time.Millisecond*560, time.Second*30, func(ctx context.Context) error {
				for _, port := range cmd.WaitPorts {
					if testutils.IsPortAvailable(port) {
						return errors.Annotatef(consts.ErrPortNotListened, "port:%d", port)
					}
				}
				return nil
			}, utils.RetryAnyError); err != nil {
				waitress <- err
			}
			close(waitress)
		}()
	}

	select {
	case err := <-waitress:
		if err != nil {
			glog.Errorf("[StartProcess] command '%s'%s failed to start: '%v'", cmdString, desc, err)
			return err
		}
		select {
		case <-cmd.stopped:
			glog.Errorf("[StartProcess] command '%s'%s failed", cmdString, desc)
			return errors.Annotatef(consts.ErrProcessFailedToStart, "error of '%s': '%v'", cmdString, cmdErr)
		default:
			cmd.NotifyStart()
			return nil
		}
	case <-cmd.stopped:
		glog.Errorf("[StartProcess] command '%s'%s failed", cmdString, desc)
		return errors.Annotatef(consts.ErrProcessFailedToStart, "publish: %v, error of '%s': '%v'", cmd.WaitPorts, cmdString, cmdErr)
	}
}

func (cmd *Command) ProcessName() string {
	return cmd.cmd.Args[0]
}

func (cmd *Command) IsFake() bool {
	return cmd.ProcessName() == "sleep"
}

func (cmd *Command) FullName() string {
	return strings.Join(cmd.cmd.Args, " ")
}

func (cmd *Command) StartPeriod() time.Duration {
	startTime, stopTime := cmd.StartTime(), cmd.StopTime()
	if startTime == nil {
		return 0
	}
	if stopTime == nil {
		return time.Since(*startTime)
	}
	return stopTime.Sub(*startTime)
}

func (cmd *Command) StartTime() *time.Time {
	v := cmd.startTime.Load()
	if v == nil {
		return nil
	}
	return v.(*time.Time)
}

func (cmd *Command) StopTime() *time.Time {
	v := cmd.stopTime.Load()
	if v == nil {
		return nil
	}
	return v.(*time.Time)
}

func (cmd *Command) SetKilled() {
	cmd.killed.Store(true)
}

func (cmd *Command) IsKilled() bool {
	v := cmd.killed.Load()
	if v == nil {
		return false
	}
	return v.(bool)
}

func (cmd *Command) Done() <-chan struct{} {
	return cmd.stopped
}

func (cmd *Command) NotifyStop() {
	cmd.mutex.Lock()
	defer cmd.mutex.Unlock()

	if !cmd.HasStopped() {
		close(cmd.stopped)
	}
}

func (cmd *Command) NotifyStart() {
	cmd.mutex.Lock()
	defer cmd.mutex.Unlock()

	if cmd.HasStopped() {
		return // not allow stop->start
	}

	if !cmd.HasStarted() {
		close(cmd.started)
	}
}

func (cmd *Command) Status() Status {
	hasStarted, hasStopped := cmd.HasStarted(), cmd.HasStopped()

	if hasStopped {
		return StatusStopped
	}
	if hasStarted {
		return StatusRunning
	}
	return StatusUninitialized
}

func (cmd *Command) HasStarted() bool {
	select {
	case <-cmd.started:
		return true
	default:
		return false
	}
}

func (cmd *Command) HasStopped() bool {
	select {
	case <-cmd.stopped:
		return true
	default:
		return false
	}
}

func (cmd *Command) Kill() error {
	status := cmd.Status()
	if status == StatusUninitialized {
		return nil
	}

	cmd.SetKilled()
	if status == StatusRunning {
		var killErr error
		if err := utils.WithRetry(1, 10, func(_ context.Context) error {
			killErr = cmd.cmd.Process.Kill()
			if killErr == nil || cmd.Status() == StatusStopped {
				return nil
			}
			return killErr
		}); err != nil && cmd.Status() == StatusRunning {
			glog.Warningf("[Command][Stop] failed to stop command %s, err: '%v'", cmd.FullName(), killErr)
			return err
		}
	}

	ctx, canceller := context.WithTimeout(context.Background(), consts.StopContainerTimeout)
	defer canceller()

	select {
	case <-ctx.Done():
		glog.Warningf("[Command][Stop] wait command %s exited failed: '%v'", cmd.FullName(), ctx.Err())
		return errors.Wrap(consts.ErrFailedToStopContainer, ctx.Err())
	case <-cmd.stopped:
		return nil
	}
}
