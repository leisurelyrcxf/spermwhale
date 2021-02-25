package txn

import (
	"context"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"
	"github.com/leisurelyrcxf/spermwhale/proto/txnpb"
	"github.com/leisurelyrcxf/spermwhale/types"
)

type Stub struct {
	m *TransactionManager
}

func (s *Stub) Begin(ctx context.Context, req *txnpb.BeginRequest) (*txnpb.BeginResponse, error) {
	txn, err := s.m.BeginTxn(ctx)
	if err != nil {
		return &txnpb.BeginResponse{Err: commonpb.ToPBError(err)}, nil
	}
	return &txnpb.BeginResponse{
		TxnId: txn.ID,
	}, nil
}

func (s *Stub) Get(ctx context.Context, req *txnpb.GetRequest) (*txnpb.GetResponse, error) {
	txn, err := s.m.GetTxn(req.TxnId)
	if err != nil {
		return &txnpb.GetResponse{Err: commonpb.ToPBError(err)}, nil
	}
	val, err := txn.Get(ctx, req.Key)
	if err != nil {
		return &txnpb.GetResponse{Err: commonpb.ToPBError(err)}, nil
	}
	return &txnpb.GetResponse{
		V: commonpb.ToPBValue(val),
	}, nil
}

func (s *Stub) Set(ctx context.Context, req *txnpb.SetRequest) (*txnpb.SetResponse, error) {
	txn, err := s.m.GetTxn(req.TxnId)
	if err != nil {
		return &txnpb.SetResponse{Err: commonpb.ToPBError(err)}, nil
	}
	return &txnpb.SetResponse{Err: commonpb.ToPBError(
		txn.Set(ctx, req.Key, req.Value)),
	}, nil
}

func (s *Stub) Rollback(ctx context.Context, req *txnpb.RollbackRequest) (*txnpb.RollbackResponse, error) {
	txn, err := s.m.GetTxn(req.TxnId)
	if err != nil {
		return &txnpb.RollbackResponse{Err: commonpb.ToPBError(err)}, nil
	}
	return &txnpb.RollbackResponse{Err: commonpb.ToPBError(
		txn.Rollback(ctx))}, nil
}

func (s *Stub) Commit(ctx context.Context, req *txnpb.CommitRequest) (*txnpb.CommitResponse, error) {
	txn, err := s.m.GetTxn(req.TxnId)
	if err != nil {
		return &txnpb.CommitResponse{Err: commonpb.ToPBError(err)}, nil
	}
	return &txnpb.CommitResponse{Err: commonpb.ToPBError(
		txn.Commit(ctx))}, nil
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
	workerNumber int,
	port int) *Server {
	grpcServer := grpc.NewServer()

	txnpb.RegisterTxnServer(grpcServer, &Stub{
		m: NewTransactionManager(kv, cfg, workerNumber),
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
