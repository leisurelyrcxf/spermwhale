package tablet

import (
	"github.com/leisurelyrcxf/spermwhale/kv"
	"github.com/leisurelyrcxf/spermwhale/models"
	fsclient "github.com/leisurelyrcxf/spermwhale/models/client/fs"
	"github.com/leisurelyrcxf/spermwhale/mvcc/impl/memory"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils/network"
)

type Server struct {
	kv.Server

	gid   int
	store *models.Store
}

func NewServer(port int, cfg types.TxnConfig, gid int, store *models.Store) *Server {
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

func (s *Server) online() error {
	localAddr, err := network.GetLocalAddr(s.targetAddr())
	if err != nil {
		return err
	}
	return s.store.UpdateGroup(
		&models.Group{
			Id:         s.gid,
			ServerAddr: localAddr,
		},
	)
}

func (s *Server) targetAddr() string {
	if _, isFsClient := s.store.Client().(*fsclient.Client); isFsClient {
		return "8.8.8.8:53"
	}
	return s.store.Client().AddrList()
}
