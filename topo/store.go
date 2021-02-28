// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topo

import (
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"time"

	"github.com/leisurelyrcxf/spermwhale/topo/client/common"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/topo/client"
)

const SpermwhaleDir = "/spermwhale"

func ClusterDir(clusterName string) string {
	return filepath.Join(SpermwhaleDir, clusterName)
}

func LockPath(clusterName string) string {
	return filepath.Join(SpermwhaleDir, clusterName, "topom")
}

func GroupDir(clusterName string) string {
	return filepath.Join(SpermwhaleDir, clusterName, "group")
}

func GroupPath(clusterName string, gid int) string {
	return filepath.Join(SpermwhaleDir, clusterName, "group", fmt.Sprintf("group-%04d", gid))
}

func OraclePath(clusterName string) string {
	return filepath.Join(SpermwhaleDir, clusterName, "oracle")
}

type Store struct {
	client      client.Client
	clusterName string
}

func NewStore(client client.Client, clusterName string) *Store {
	return &Store{client, clusterName}
}

func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) Client() client.Client {
	return s.client
}

func (s *Store) LockPath() string {
	return LockPath(s.clusterName)
}

func (s *Store) GroupDir() string {
	return GroupDir(s.clusterName)
}

func (s *Store) GroupPath(gid int) string {
	return GroupPath(s.clusterName, gid)
}

func (s *Store) OraclePath() string {
	return OraclePath(s.clusterName)
}

func (s *Store) Lock() (err error) {
	for i := 0; i < 60; i++ {
		if err = s.client.Create(s.LockPath(), []byte{}); err != nil {
			glog.Errorf("lock failed: %v", err)
			time.Sleep(time.Second)
			continue
		}
		return nil
	}
	return err
}

func (s *Store) Acquire() error {
	return s.client.Create(s.LockPath(), []byte{})
}

func (s *Store) Unlock() error {
	return s.client.Delete(s.LockPath())
}

func (s *Store) ListGroup() (map[int]*Group, error) {
	paths, err := s.client.List(s.GroupDir())
	if err != nil {
		return nil, err
	}
	group := make(map[int]*Group)
	for _, path := range paths {
		b, err := s.client.Read(path, true)
		if err != nil {
			return nil, err
		}
		g := &Group{}
		if err := jsonDecode(g, b); err != nil {
			return nil, err
		}
		group[g.Id] = g
	}
	return group, nil
}

func (s *Store) LoadGroup(gid int, must bool) (*Group, error) {
	b, err := s.client.Read(s.GroupPath(gid), must)
	if err != nil || b == nil {
		return nil, err
	}
	g := &Group{}
	if err := jsonDecode(g, b); err != nil {
		return nil, err
	}
	return g, nil
}

func (s *Store) UpdateGroup(g *Group) error {
	return s.client.Update(s.GroupPath(g.Id), g.Encode())
}

func (s *Store) DeleteGroup(gid int) error {
	return s.client.Delete(s.GroupPath(gid))
}

func (s *Store) UpdateTimestamp(ts uint64) error {
	return s.client.Update(s.OraclePath(), []byte(strconv.FormatUint(ts, 10)))
}

func (s *Store) LoadTimestamp() (uint64, error) {
	val, err := s.client.Read(s.OraclePath(), true)
	if err != nil && err != common.ErrKeyNotExists {
		return 0, err
	}
	if err == common.ErrKeyNotExists {
		return math.MaxUint64, nil
	}
	persisted, err := strconv.ParseUint(string(val), 10, 64)
	if err != nil {
		return 0, err
	}
	return persisted, nil
}

func (s *Store) DeleteTimestamp() error {
	return s.client.Delete(s.OraclePath())
}

func (s *Store) WithClusterLocked(f func() error) error {
	if err := s.Lock(); err != nil {
		return err
	}
	defer s.Unlock()
	return f()
}
