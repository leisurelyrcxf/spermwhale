package types

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/leisurelyrcxf/spermwhale/utils"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"

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

func (i TxnId) Age() time.Duration {
	return time.Duration(utils.GetLocalTimestamp() - i.Version())
}

type TxnManager interface {
	BeginTransaction(ctx context.Context, typ TxnType) (Txn, error)
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

func (s TxnState) IsTerminated() bool {
	return s.IsAborted() || s == TxnStateCommitted
}

type TxnType int

const (
	TxnTypeInvalid      TxnType = 0
	TxnTypeDefault      TxnType = 1
	TxnTypeReadForWrite TxnType = 2

	TxnTypeDefaultDesc      = "default"
	TxnTypeReadForWriteDesc = "read_for_write"
)

var SupportedTransactionTypesDesc = []string{"'" + TxnTypeDefaultDesc + "'", "'" + TxnTypeReadForWriteDesc + "'"}

func ParseTxnType(str string) (TxnType, error) {
	switch str {
	case TxnTypeDefaultDesc, fmt.Sprintf("%d", TxnTypeDefault):
		return TxnTypeDefault, nil
	case TxnTypeReadForWriteDesc, fmt.Sprintf("%d", TxnTypeReadForWrite):
		return TxnTypeReadForWrite, nil
	default:
		return TxnTypeInvalid, errors.Annotatef(errors.ErrInvalidRequest, "unknown txn type %s", str)
	}
}

func (t TxnType) String() string {
	switch t {
	case TxnTypeDefault:
		return TxnTypeDefaultDesc
	case TxnTypeReadForWrite:
		return TxnTypeReadForWriteDesc
	default:
		return "invalid"
	}
}

func (t TxnType) ToPB() txnpb.TxnType {
	return txnpb.TxnType(t)
}

func (t TxnType) IsReadForWrite() bool {
	return t == TxnTypeReadForWrite
}

type TxnReadOption struct {
	flag uint8
}

func NewTxnReadOption() TxnReadOption {
	return TxnReadOption{}
}

func NewTxnReadOptionFromPB(x *txnpb.TxnReadOption) TxnReadOption {
	if x == nil {
		return NewTxnReadOption()
	}
	return TxnReadOption{
		flag: x.GetFlagSafe(),
	}
}

var TxnReadOptionDesc2BitMask = map[string]uint8{
	"wait_no_write_intent": consts.CommonReadOptBitMaskWaitNoWriteIntent,
}

func GetTxnReadOptionDesc() string {
	keys := make([]string, 0, len(TxnReadOptionDesc2BitMask))
	for key, mask := range TxnReadOptionDesc2BitMask {
		keys = append(keys, fmt.Sprintf("%s(%d)", key, mask))
	}
	return strings.Join(keys, "|")
}

func ParseTxnReadOption(str string) (opt TxnReadOption, _ error) {
	parts := strings.Split(str, "|")
	for _, part := range parts {
		if mask, err := strconv.ParseInt(part, 10, 64); err == nil {
			opt.flag |= uint8(mask)
			continue
		}
		mask, ok := TxnReadOptionDesc2BitMask[part]
		if !ok {
			return opt, errors.ErrInvalidRequest
		}
		opt.flag |= mask
	}
	return opt, nil
}

func (opt TxnReadOption) ToPB() *txnpb.TxnReadOption {
	return (&txnpb.TxnReadOption{}).SetFlagSafe(opt.flag)
}

func (opt TxnReadOption) WithWaitNoWriteIntent() TxnReadOption {
	opt.flag |= consts.CommonReadOptBitMaskWaitNoWriteIntent
	return opt
}

type Txn interface {
	GetId() TxnId
	GetState() TxnState
	GetType() TxnType
	Get(ctx context.Context, key string, opt TxnReadOption) (Value, error)
	Set(ctx context.Context, key string, val []byte) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}
