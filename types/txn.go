package types

import (
	"context"
	"math"

	"github.com/leisurelyrcxf/spermwhale/proto/txnpb"
)

const (
	MaxTxnVersion = uint64(math.MaxUint64)
	MaxTxnId      = TxnId(MaxTxnVersion)
)

func SafeIncr(version *uint64) {
	if cur := *version; cur != math.MaxUint64 {
		*version = cur + 1
	}
}

type TxnId uint64

func (i TxnId) Version() uint64 {
	return uint64(i)
}

type TxnManager interface {
	BeginTransaction(ctx context.Context) (Txn, error)
	Close() error
}

type TxnState int

const (
	TxnStateInvalid     TxnState = 0
	TxnStateUncommitted TxnState = 1
	TxnStateStaging     TxnState = 2
	TxnStateCommitted   TxnState = 3
	TxnStateRollbacking TxnState = 4
	TxnStateRollbacked  TxnState = 5
)

var stateStrings = map[TxnState]string{
	TxnStateInvalid:     "'invalid'",
	TxnStateUncommitted: "'uncommitted'",
	TxnStateStaging:     "'staging'",
	TxnStateCommitted:   "'committed'",
	TxnStateRollbacking: "'rollbacking'",
	TxnStateRollbacked:  "'rollbacked'",
}

func (s TxnState) ToPB() txnpb.TxnState {
	return txnpb.TxnState(s)
}

func (s TxnState) String() string {
	return stateStrings[s]
}

func (s TxnState) IsAborted() bool {
	return s == TxnStateRollbacking || s == TxnStateRollbacked
}

type Txn interface {
	GetId() TxnId
	GetState() TxnState
	Get(ctx context.Context, key string) (Value, error)
	Set(ctx context.Context, key string, val []byte) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}
