package tablet

import (
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/leisurelyrcxf/spermwhale/kv"
	"github.com/leisurelyrcxf/spermwhale/mvcc/impl/memory"
	"github.com/leisurelyrcxf/spermwhale/topo"
	"github.com/leisurelyrcxf/spermwhale/types"
)

type Server struct {
	kv.Server

	gid   int
	store *topo.Store
}

func NewServer(port int, cfg types.TxnConfig, gid int, store *topo.Store) *Server {
	db := memory.NewDB()
	s := &Server{
		Server: kv.NewServer(port, NewKVCC(db, cfg), false),
		gid:    gid,
		store:  store,
	}
	s.Server.SetBeforeStart(func() error {
		return s.online()
	})
	return s
}

func NewServerForTesting(port int, cfg types.TxnConfig, gid int, store *topo.Store) *Server {
	db := memory.NewDB()
	s := &Server{
		Server: kv.NewServer(port, NewKVCCForTesting(db, cfg), false),
		gid:    gid,
		store:  store,
	}
	s.Server.SetBeforeStart(func() error {
		return s.online()
	})
	return s
}

func (s *Server) Close() error {
	return errors.Wrap(s.store.Close(), s.Server.Close())
}

func (s *Server) online() error {
	localIP, err := s.store.GetLocalIP()
	if err != nil {
		return err
	}
	return s.store.UpdateGroup(
		&topo.Group{
			Id:         s.gid,
			ServerAddr: fmt.Sprintf("%s:%d", localIP, s.Port),
		},
	)
}
