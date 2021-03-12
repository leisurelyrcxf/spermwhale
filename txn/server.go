package txn

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/leisurelyrcxf/spermwhale/topo"

	"google.golang.org/grpc"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/proto/txnpb"
	"github.com/leisurelyrcxf/spermwhale/types"
)

type Stub struct {
	m *TransactionManager
}

func (s *Stub) Begin(ctx context.Context, req *txnpb.BeginRequest) (*txnpb.BeginResponse, error) {
	txn, err := s.m.BeginTransaction(ctx)
	if err != nil {
		return &txnpb.BeginResponse{Err: errors.ToPBError(err)}, nil
	}
	return &txnpb.BeginResponse{
		Txn: txn.(*Txn).ToPB(),
	}, nil
}

func (s *Stub) Get(ctx context.Context, req *txnpb.TxnGetRequest) (*txnpb.TxnGetResponse, error) {
	txn, err := s.m.GetTxn(types.TxnId(req.TxnId))
	if err != nil {
		return &txnpb.TxnGetResponse{
			Txn: InvalidTransactionInfo(types.TxnId(req.TxnId)).ToPB(), Err: errors.ToPBError(err)}, nil
	}
	val, err := txn.Get(ctx, req.Key, types.NewTxnReadOptionFromPB(req.Opt))
	if err != nil {
		return &txnpb.TxnGetResponse{Txn: txn.ToPB(), Err: errors.ToPBError(err)}, nil
	}
	return &txnpb.TxnGetResponse{
		Txn: txn.ToPB(),
		V:   val.ToPB(),
	}, nil
}

func (s *Stub) Set(ctx context.Context, req *txnpb.TxnSetRequest) (*txnpb.TxnSetResponse, error) {
	txn, err := s.m.GetTxn(types.TxnId(req.TxnId))
	if err != nil {
		return &txnpb.TxnSetResponse{
			Txn: InvalidTransactionInfo(types.TxnId(req.TxnId)).ToPB(), Err: errors.ToPBError(err)}, nil
	}
	err = txn.Set(ctx, req.Key, req.Value)
	return &txnpb.TxnSetResponse{
		Txn: txn.ToPB(),
		Err: errors.ToPBError(err),
	}, nil
}

func (s *Stub) Rollback(ctx context.Context, req *txnpb.RollbackRequest) (*txnpb.RollbackResponse, error) {
	txn, err := s.m.GetTxn(types.TxnId(req.TxnId))
	if err != nil {
		return &txnpb.RollbackResponse{
			Txn: InvalidTransactionInfo(types.TxnId(req.TxnId)).ToPB(), Err: errors.ToPBError(err)}, nil
	}
	err = txn.Rollback(ctx)
	return &txnpb.RollbackResponse{
		Txn: txn.ToPB(),
		Err: errors.ToPBError(err)}, nil
}

func (s *Stub) Commit(ctx context.Context, req *txnpb.CommitRequest) (*txnpb.CommitResponse, error) {
	txn, err := s.m.GetTxn(types.TxnId(req.TxnId))
	if err != nil {
		return &txnpb.CommitResponse{
			Txn: InvalidTransactionInfo(types.TxnId(req.TxnId)).ToPB(), Err: errors.ToPBError(err)}, nil
	}
	err = txn.Commit(ctx)
	return &txnpb.CommitResponse{
		Txn: txn.ToPB(),
		Err: errors.ToPBError(err),
	}, nil
}

type Server struct {
	Port int

	tm         *TransactionManager
	grpcServer *grpc.Server
	Done       chan struct{}
}

func NewServer(
	port int,
	kv types.KVCC,
	cfg types.TxnManagerConfig,
	clearWorkerNumber, ioWorkerNumber int,
	store *topo.Store) (*Server, error) {
	tm, err := NewTransactionManagerWithCluster(kv, cfg, clearWorkerNumber, ioWorkerNumber, store)
	if err != nil {
		return nil, err
	}
	grpcServer := grpc.NewServer()
	txnpb.RegisterTxnServiceServer(grpcServer, &Stub{m: tm})

	return &Server{
		Port: port,

		tm:         tm,
		grpcServer: grpcServer,
		Done:       make(chan struct{}),
	}, nil
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		glog.Errorf("failed to listen: %v", err)
		return err
	}

	go func() {
		defer close(s.Done)

		if err := s.grpcServer.Serve(lis); err != nil {
			glog.Errorf("txn server 0.0.0.0:%d serve failed: %v", s.Port, err)
		} else {
			glog.Infof("txn server 0.0.0.0:%d terminated successfully", s.Port)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	return nil
}

func (s *Server) Close() error {
	s.grpcServer.Stop()
	<-s.Done
	return s.tm.Close()
}
