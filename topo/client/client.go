package client

import (
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"

	fsclient "github.com/leisurelyrcxf/spermwhale/topo/client/fs"

	"github.com/leisurelyrcxf/spermwhale/topo/client/etcdclient"
)

type Client interface {
	AddrList() string

	Create(path string, data []byte) error
	Update(path string, data []byte) error
	Delete(path string) error
	Rmdir(dir string) error

	Read(path string, must bool) ([]byte, error)
	List(path string) ([]string, error)

	WatchOnce(path string) (<-chan struct{}, error)

	Close() error
}

func NewClient(coordinator string, addrList string, auth string, timeout time.Duration) (Client, error) {
	switch coordinator {
	case "etcdclient":
		return etcdclient.New(addrList, auth, timeout)
	case "fs", "filesystem":
		return fsclient.New(addrList)
	}
	return nil, errors.Errorf("invalid coordinator name = %s", coordinator)
}

func TargetAddr(c Client) string {
	if _, isFsClient := c.(*fsclient.Client); isFsClient {
		return "8.8.8.8:53"
	}
	return c.AddrList()
}
