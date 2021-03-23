package types

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/proto/txnpb"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

const (
	MaxTxnVersionDate = "2200-01-01T00:00:00Z"
	MaxTxnVersion     = uint64(7258118400000000000)
	MaxTxnId          = TxnId(MaxTxnVersion)
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

func (i TxnId) Max(another TxnId) TxnId {
	if i > another {
		return i
	}
	return another
}

type TxnInternalVersion uint8

const (
	TxnInternalVersionMin             TxnInternalVersion = consts.MinTxnInternalVersion
	TxnInternalVersionMax             TxnInternalVersion = consts.MaxTxnInternalVersion
	TxnInternalVersionPositiveInvalid TxnInternalVersion = consts.PositiveInvalidTxnInternalVersion
)

func (v TxnInternalVersion) IsValid() bool {
	return v != 0 && v != TxnInternalVersionPositiveInvalid
}

// AtomicTxnId is a wrapper with a simpler interface around atomic.(Add|Store|Load|CompareAndSwap)TxnId functions.
type AtomicTxnId struct {
	uint64
}

// NewAtomicTxnId initializes a new AtomicTxnId with a given value.
func NewAtomicTxnId(n uint64) AtomicTxnId {
	return AtomicTxnId{n}
}

// Set atomically sets n as new value.
func (i *AtomicTxnId) Set(id TxnId) {
	atomic.StoreUint64(&i.uint64, uint64(id))
}

// Get atomically returns the current value.
func (i *AtomicTxnId) Get() TxnId {
	return TxnId(atomic.LoadUint64(&i.uint64))
}

func (i *AtomicTxnId) SetIfBiggerUnsafe(id TxnId) {
	if id > i.Get() {
		i.Set(id)
	}
}

type TxnManager interface {
	BeginTransaction(ctx context.Context, typ TxnType) (Txn, error)
	Close() error
}

type TxnState uint8

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

type TxnType uint8

const (
	TxnTypeInvalid      TxnType = 0
	TxnTypeDefault      TxnType = 1
	TxnTypeReadForWrite TxnType = 2
	TxnTypeSnapshotRead TxnType = 3

	TxnTypeDefaultDesc      = "default"
	TxnTypeReadForWriteDesc = "read_for_write"
	TxnTypeSnapshotReadDesc = "snapshot_read"
)

var SupportedTransactionTypesDesc = []string{
	"'" + TxnTypeDefaultDesc + "'",
	"'" + TxnTypeReadForWriteDesc + "'",
	"'" + TxnTypeSnapshotReadDesc + "'"}

func ParseTxnType(str string) (TxnType, error) {
	switch str {
	case TxnTypeDefaultDesc, fmt.Sprintf("%d", TxnTypeDefault):
		return TxnTypeDefault, nil
	case TxnTypeReadForWriteDesc, fmt.Sprintf("%d", TxnTypeReadForWrite):
		return TxnTypeReadForWrite, nil
	case TxnTypeSnapshotReadDesc, fmt.Sprintf("%d", TxnTypeSnapshotRead):
		return TxnTypeSnapshotRead, nil
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
	case TxnTypeSnapshotRead:
		return TxnTypeSnapshotReadDesc
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

func (t TxnType) IsSnapshotRead() bool {
	return t == TxnTypeSnapshotRead
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

func (opt TxnReadOption) IsWaitNoWriteIntent() bool {
	return opt.flag&consts.CommonReadOptBitMaskWaitNoWriteIntent == consts.CommonReadOptBitMaskWaitNoWriteIntent
}

type Txn interface {
	GetId() TxnId
	GetState() TxnState
	GetType() TxnType
	MGet(ctx context.Context, keys []string, opt TxnReadOption) (values []Value, err error)
	Get(ctx context.Context, key string, opt TxnReadOption) (Value, error)
	Set(ctx context.Context, key string, val []byte) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}
