package txn

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"google.golang.org/grpc"

	"github.com/golang/glog"

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
			Txn: NewTransactionInfo(types.TxnId(req.TxnId), types.TxnStateInvalid).ToPB(), Err: errors.ToPBError(err)}, nil
	}
	val, err := txn.Get(ctx, req.Key)
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
			Txn: NewTransactionInfo(types.TxnId(req.TxnId), types.TxnStateInvalid).ToPB(), Err: errors.ToPBError(err)}, nil
	}
	return &txnpb.TxnSetResponse{
		Txn: txn.ToPB(),
		Err: errors.ToPBError(txn.Set(ctx, req.Key, req.Value)),
	}, nil
}

func (s *Stub) Rollback(ctx context.Context, req *txnpb.RollbackRequest) (*txnpb.RollbackResponse, error) {
	txn, err := s.m.GetTxn(types.TxnId(req.TxnId))
	if err != nil {
		return &txnpb.RollbackResponse{
			Txn: NewTransactionInfo(types.TxnId(req.TxnId), types.TxnStateInvalid).ToPB(), Err: errors.ToPBError(err)}, nil
	}
	return &txnpb.RollbackResponse{
		Txn: txn.ToPB(),
		Err: errors.ToPBError(txn.Rollback(ctx))}, nil
}

func (s *Stub) Commit(ctx context.Context, req *txnpb.CommitRequest) (*txnpb.CommitResponse, error) {
	txn, err := s.m.GetTxn(types.TxnId(req.TxnId))
	if err != nil {
		return &txnpb.CommitResponse{
			Txn: NewTransactionInfo(types.TxnId(req.TxnId), types.TxnStateInvalid).ToPB(), Err: errors.ToPBError(err)}, nil
	}
	return &txnpb.CommitResponse{
		Txn: txn.ToPB(),
		Err: errors.ToPBError(txn.Commit(ctx)),
	}, nil
}

type Server struct {
	kv         types.KV
	grpcServer *grpc.Server
	port       int
	Done       chan struct{}
}

func NewServer(
	kv types.KV,
	cfg types.TxnConfig,
	clearWorkerNumber, ioWorkerNumber int,
	port int) *Server {
	grpcServer := grpc.NewServer()

	txnpb.RegisterTxnServiceServer(grpcServer, &Stub{
		m: NewTransactionManager(kv, cfg, clearWorkerNumber, ioWorkerNumber),
	})

	return &Server{
		kv:         kv,
		grpcServer: grpcServer,
		port:       port,
		Done:       make(chan struct{}),
	}
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		glog.Errorf("failed to listen: %v", err)
		return err
	}

	go func() {
		defer close(s.Done)

		if err := s.grpcServer.Serve(lis); err != nil {
			glog.Errorf("tablet serve failed: %v", err)
		} else {
			glog.Infof("tablet server terminated successfully")
		}
	}()

	time.Sleep(100 * time.Millisecond)
	return nil
}

func (s *Server) Stop() {
	s.grpcServer.Stop()
	_ = s.kv.Close()
	<-s.Done
}
