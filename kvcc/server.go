package kvcc

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"

	"github.com/golang/glog"

	"google.golang.org/grpc"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/proto/kvccpb"
	"github.com/leisurelyrcxf/spermwhale/topo"
	"github.com/leisurelyrcxf/spermwhale/types"
)

type Stub struct {
	KVCC types.KVCC
}

func (stub *Stub) Get(ctx context.Context, req *kvccpb.KVCCGetRequest) (*kvccpb.KVCCGetResponse, error) {
	opt := types.NewKVCCReadOptionFromPB(req.Opt)
	vv, err := stub.KVCC.Get(ctx, req.Key, *opt)
	assert.Must(err != nil || !vv.IsEmpty())
	//noinspection ALL
	return &kvccpb.KVCCGetResponse{
		V:   vv.ToPB(),
		Err: errors.ToPBError(err),
	}, nil
}

func (stub *Stub) Set(ctx context.Context, req *kvccpb.KVCCSetRequest) (*kvccpb.KVCCSetResponse, error) {
	if err := req.Validate(); err != nil {
		return &kvccpb.KVCCSetResponse{Err: errors.ToPBError(err)}, nil
	}
	err := stub.KVCC.Set(ctx, req.Key, types.NewValueFromPB(req.Value), types.NewKVCCWriteOptionFromPB(req.Opt))
	return &kvccpb.KVCCSetResponse{Err: errors.ToPBError(err)}, nil
}

func (stub *Stub) KeyVersionCount(ctx context.Context, req *kvccpb.KVCCVersionCountRequest) (*kvccpb.KVCCVersionCountResponse, error) {
	count, err := stub.KVCC.KeyVersionCount(ctx, req.Key)
	return &kvccpb.KVCCVersionCountResponse{VersionCount: count, Err: errors.ToPBError(err)}, nil
}

func (stub *Stub) UpdateMeta(ctx context.Context, req *kvccpb.KVCCUpdateMetaRequest) (*kvccpb.KVCCUpdateMetaResponse, error) {
	err := stub.KVCC.UpdateMeta(ctx, req.Key, req.Version, types.NewKVCCCUpdateMetaOptionFromPB(req.Opt))
	return &kvccpb.KVCCUpdateMetaResponse{Err: errors.ToPBError(err)}, nil
}

func (stub *Stub) RollbackKey(ctx context.Context, req *kvccpb.KVCCRollbackKeyRequest) (*kvccpb.KVCCRollbackKeyResponse, error) {
	err := stub.KVCC.RollbackKey(ctx, req.Key, req.Version, types.NewKVCCCRollbackKeyOptionFromPB(req.Opt))
	return &kvccpb.KVCCRollbackKeyResponse{Err: errors.ToPBError(err)}, nil
}

func (stub *Stub) RemoveTxnRecord(ctx context.Context, req *kvccpb.KVCCRemoveTxnRecordRequest) (*kvccpb.KVCCRemoveTxnRecordResponse, error) {
	err := stub.KVCC.RemoveTxnRecord(ctx, req.Version, types.NewKVCCCRemoveTxnRecordOptionFromPB(req.Opt))
	return &kvccpb.KVCCRemoveTxnRecordResponse{Err: errors.ToPBError(err)}, nil
}

type Server struct {
	Port int

	grpcServer *grpc.Server
	Stub       *Stub

	gid   int
	store *topo.Store

	Done chan struct{}
}

// readOnly indicate this is outer service
func NewServer(port int, db types.KV, cfg types.TabletTxnManagerConfig, gid int, store *topo.Store) *Server {
	grpcServer := grpc.NewServer()
	kvcc := NewKVCC(db, cfg)
	stub := &Stub{KVCC: kvcc}

	kvccpb.RegisterKVCCServer(grpcServer, stub)
	return &Server{
		Port: port,

		grpcServer: grpcServer,
		Stub:       stub,

		gid:   gid,
		store: store,

		Done: make(chan struct{}),
	}
}

func (s *Server) Start() error {
	if err := s.online(); err != nil {
		return err
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		glog.Errorf("failed to listen: %v", err)
		return err
	}

	go func() {
		defer close(s.Done)

		if err := s.grpcServer.Serve(lis); err != nil {
			glog.Errorf("kv server 0.0.0.0:%d serve failed: %v", s.Port, err)
		} else {
			glog.V(6).Infof("kv server 0.0.0.0:%d terminated successfully", s.Port)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	return nil
}

func (s *Server) Close() (err error) {
	storeErr := s.store.Close()
	s.grpcServer.Stop()
	<-s.Done
	kvErr := s.Stub.KVCC.Close()
	return errors.Wrap(storeErr, kvErr)
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
