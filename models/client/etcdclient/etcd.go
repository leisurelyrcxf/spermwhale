// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package etcdclient

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/models/client/common"
	"go.etcd.io/etcd/clientv3"
)

var ErrClosedClient = errors.New("use of closed etcdclient client")

type Client struct {
	addrlist string
	sync.Mutex
	kapi clientv3.KV
	c    *clientv3.Client

	closed  bool
	timeout time.Duration

	cancel  context.CancelFunc
	context context.Context

	leaseID clientv3.LeaseID
}

func New(addrlist string, auth string, timeout time.Duration) (*Client, error) {
	endpoints := strings.Split(addrlist, ",")
	for i, s := range endpoints {
		if s != "" && !strings.HasPrefix(s, "http://") {
			endpoints[i] = "http://" + s
		}
	}
	if timeout <= 0 {
		timeout = time.Second * 5
	}

	config := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second * 5,
	}

	if auth != "" {
		split := strings.SplitN(auth, ":", 2)
		if len(split) != 2 || split[0] == "" {
			return nil, fmt.Errorf("invalid auth")
		}
		config.Username = split[0]
		config.Password = split[1]
	}

	c, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}

	client := &Client{
		addrlist: addrlist,
		kapi:     clientv3.NewKV(c), timeout: timeout, c: c,
	}
	client.context, client.cancel = context.WithCancel(context.Background())
	return client, nil
}

func (c *Client) AddrList() string {
	return c.addrlist
}

func (c *Client) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.cancel()
	return c.c.Close()
}

func (c *Client) isClosed() bool {
	c.Lock()
	defer c.Unlock()
	return c.closed
}

func (c *Client) newContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.context, c.timeout)
}

func (c *Client) Create(path string, data []byte) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}

	ctx, cancel := c.newContext()
	defer cancel()
	glog.Infof("etcdclient create node %s", path)

	req := clientv3.OpPut(path, string(data))
	cond := clientv3.Compare(clientv3.Version(path), "=", 0)
	resp, err := c.kapi.Txn(ctx).If(cond).Then(req).Commit()
	if err != nil {
		glog.Infof("etcdclient create node %s failed: %s", path, err)
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		err = common.ErrKeyAlreadyExists
		glog.Infof("etcdclient create node %s failed: %s", path, err)
		return err
	}
	glog.Infof("etcdclientv3 create OK")
	return nil
}

func (c *Client) Update(path string, data []byte) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	glog.Infof("etcdclient update node %s", path)
	_, err := c.kapi.Put(cntx, path, string(data))
	if err != nil {
		glog.Infof("etcdclient update node %s failed: %s", path, err)
		return errors.Trace(err)
	}
	glog.Infof("etcdclient update OK")
	return nil
}

func (c *Client) Delete(path string) error {
	return c.delete(path, "delete node")
}

func (c *Client) Rmdir(dir string) error {
	return c.delete(dir, "rmdir", clientv3.WithPrefix())
}

func (c *Client) delete(path string, desc string, opts ...clientv3.OpOption) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}
	glog.Infof("etcdclient %s %s", desc, path)
	cntx, cancel := c.newContext()
	defer cancel()
	if _, err := c.kapi.Delete(cntx, path, opts...); err != nil {
		glog.Errorf("etcdclient %s %s failed: %s", desc, path, err)
		return errors.Trace(err)
	}
	glog.Infof("etcdclient %s %s OK", desc, path)
	return nil
}

func (c *Client) Read(path string, must bool) ([]byte, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	// By default, etcdclient clientv3 use same config as old 'client.GetOptions{Quorum: true}'
	r, err := c.kapi.Get(cntx, path)
	if err != nil {
		glog.Infof("etcdclient read node %s failed: %s", path, err)
		return nil, errors.Trace(err)
	}
	// Otherwise should return error instead.
	if len(r.Kvs) == 0 {
		if must {
			return nil, common.ErrKeyNotExists
		}
		return nil, nil
	}
	return r.Kvs[0].Value, nil
}

func (c *Client) List(path string) ([]string, error) {
	c.Lock()
	defer c.Unlock()
	return c.listLocked(path)
}

func (c *Client) listLocked(path string) ([]string, error) {
	if c.closed {
		return nil, errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithKeysOnly(),
	}
	r, err := c.kapi.Get(cntx, path, opts...)
	switch {
	case err != nil:
		glog.Infof("etcdclient list node %s failed: %s", path, err)
		return nil, errors.Trace(err)
	default:
		var (
			listedPathSet = make(map[string]struct{})
			listedPaths   []string
		)
		for _, kv := range r.Kvs {
			key := string(kv.Key)
			if len(key) < len(path) {
				return nil, errors.Errorf("impossible: key_len(%d) < path_len(%d)", len(kv.Key), len(path))
			}
			remain := key[len(path):]
			if strings.HasPrefix(remain, "/") {
				remain = remain[1:]
			}
			if remain == "" {
				continue
			}
			if firstSlashIdx := strings.IndexByte(remain, '/'); firstSlashIdx != -1 {
				listedPathSet[remain[:firstSlashIdx]] = struct{}{}
			} else {
				listedPathSet[remain] = struct{}{}
			}
		}
		for listedPath := range listedPathSet {
			listedPaths = append(listedPaths, listedPath)
		}
		sort.Strings(listedPaths)
		for idx := range listedPaths {
			listedPaths[idx] = filepath.Join(path, listedPaths[idx])
		}
		return listedPaths, nil
	}
}

func (c *Client) MkDir(path string) error {
	return c.Create(path, []byte{})
}

func (c *Client) WatchOnce(path string) (<-chan struct{}, error) {
	err := c.MkDir(path)
	if err != nil && err != common.ErrKeyAlreadyExists {
		return nil, err
	}

	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, errors.Trace(ErrClosedClient)
	}

	glog.Infof("etcd watch node %s", path)

	signal := make(chan struct{})
	watched := make(chan struct{})
	go func() {
		defer close(signal)

		cancellableCtx, canceller := context.WithCancel(clientv3.WithRequireLeader(c.context))
		// GC watched chan, otherwise will memory leak.
		defer canceller()
		ch := c.c.Watch(cancellableCtx, path, clientv3.WithPrefix())
		close(watched)
		for resp := range ch {
			for _, event := range resp.Events {
				glog.Infof("etcd watch node %s update, event: %v", path, event)
				return
			}
		}
	}()

	select {
	case <-time.After(c.timeout):
		return nil, fmt.Errorf("watch timeouted")
	case <-watched:
		break
	}

	glog.Info("etcd watch OK")
	return signal, nil
}
