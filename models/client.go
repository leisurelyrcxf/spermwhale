package models

import (
	"time"

	fsclient "github.com/leisurelyrcxf/spermwhale/models/fs"

	"github.com/leisurelyrcxf/spermwhale/models/etcdclient"
	"github.com/leisurelyrcxf/spermwhale/utils/errors"
)

type Client interface {
	Create(path string, data []byte) error
	Update(path string, data []byte) error
	Delete(path string) error
	Rmdir(dir string) error

	Read(path string, must bool) ([]byte, error)
	List(path string) ([]string, error)

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
