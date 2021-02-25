package consts

import (
	"errors"
	"syscall"
	"time"
)

const (
	ContainerDir = "/tmp/containers"

	NodeName = "testnode"

	WatchAlivePeriod     = time.Second * 2
	StopContainerTimeout = 30 * time.Second

	EPERM = syscall.Errno(0x1)

	MaxPort = 65535

	DefaultWorkDir = "/"
)

var (
	// ErrNotSupported error
	ErrNotSupported = errors.New("not implemented by mocked orchestrator")
	// ErrProcessFailedToStart error
	ErrProcessFailedToStart = errors.New("process failed to start")
	// ErrFailedToStopContainer error
	ErrFailedToStopContainer = errors.New("failed to stop container")
	// ErrPortNotListened error
	ErrPortNotListened = errors.New("port not listened")
)
