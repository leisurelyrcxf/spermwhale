package types

import (
	"context"
	"fmt"
	"hash/crc32"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"

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

func (i TxnId) String() string {
	return fmt.Sprintf("txn-%d", i)
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

type TxnInternalVersion uint8

const (
	TxnInternalVersionMin             TxnInternalVersion = consts.MinTxnInternalVersion
	TxnInternalVersionMax             TxnInternalVersion = consts.MaxTxnInternalVersion
	TxnInternalVersionPositiveInvalid TxnInternalVersion = consts.PositiveInvalidTxnInternalVersion
)

func (v TxnInternalVersion) IsValid() bool {
	return v != 0 && v != TxnInternalVersionPositiveInvalid
}

type TxnKeyUnion struct {
	Key   string
	TxnId TxnId
}

func (tk TxnKeyUnion) Hash() uint64 {
	if tk.Key != "" {
		//assert.Must(tk.TxnId == 0)
		return uint64(crc32.ChecksumIEEE([]byte(tk.Key)))
	}
	// must be a transaction record
	assert.Must(tk.TxnId != 0)
	return tk.TxnId.Version()
}

func (tk TxnKeyUnion) String() string {
	if tk.Key != "" {
		return "key-" + tk.Key
	}
	return "txn-record"
}

type TxnManager interface {
	BeginTransaction(ctx context.Context, typ TxnType, snapshotVersion uint64) (Txn, error)
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

func (s TxnState) IsStaging() bool {
	return s == TxnStateStaging
}

func (s TxnState) IsCommitted() bool {
	return s == TxnStateCommitted
}

func (s TxnState) IsAborted() bool {
	return s == TxnStateRollbacking || s == TxnStateRollbacked
}

func (s TxnState) IsTerminated() bool {
	return s.IsAborted() || s.IsCommitted()
}

type TxnKind uint8

const (
	TxnKindReadOnly  TxnKind = 1 << iota
	TxnKindReadWrite         // include read for write, read after write, write k1, read k2, etc.
	TxnKindWriteOnly
	TxnKindReadForWrite
	TxnKindSnapshotIsolation
)

func (k TxnKind) String() string {
	switch k {
	case TxnKindReadOnly:
		return "txn_readonly"
	case TxnKindReadWrite:
		return "txn_read_write"
	case TxnKindWriteOnly:
		return "txn_write_only"
	case TxnKindReadForWrite:
		return "txn_read_for_write"
	case TxnKindSnapshotIsolation:
		return "txn_snapshot_isolation"
	default:
		panic("unsupported")
	}
}

type TxnType uint8

const (
	TxnTypeInvalid      TxnType = 0
	TxnTypeDefault              = TxnType(TxnKindReadWrite)
	TxnTypeReadForWrite         = TxnType(TxnKindReadWrite | TxnKindReadForWrite)
	TxnTypeSnapshotRead         = TxnType(TxnKindReadOnly | TxnKindSnapshotIsolation)

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

func (t TxnType) ToPB() txnpb.TxnType {
	return txnpb.TxnType(t)
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
	"wait_no_write_intent": consts.TxnKVCCCommonReadOptBitMaskWaitNoWriteIntent,
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
	opt.flag |= consts.TxnKVCCCommonReadOptBitMaskWaitNoWriteIntent
	return opt
}

func (opt TxnReadOption) IsWaitNoWriteIntent() bool {
	return opt.flag&consts.TxnKVCCCommonReadOptBitMaskWaitNoWriteIntent == consts.TxnKVCCCommonReadOptBitMaskWaitNoWriteIntent
}

var (
	InvalidReadValues   = map[string]Value{"haha": {Meta: Meta{Version: 1111}}}
	IsInvalidReadValues = func(values map[string]Value) bool { return values["haha"].Version == 1111 }

	InvalidWriteValues   = map[string]Value{"biubiu": {Meta: Meta{Version: 1111}}}
	IsInvalidWriteValues = func(values map[string]Value) bool { return values["biubiu"].Version == 1111 }
)

type Txn interface {
	GetId() TxnId
	GetState() TxnState
	GetType() TxnType
	GetSnapshotVersion() uint64 // only used when txn type is snapshot
	MGet(ctx context.Context, keys []string, opt TxnReadOption) (values []Value, err error)
	Get(ctx context.Context, key string, opt TxnReadOption) (Value, error)
	Set(ctx context.Context, key string, val []byte) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error

	GetReadValues() map[string]Value
	GetWriteValues() map[string]Value
}

type RecordValuesTxn struct {
	Txn
	readValues, writeValues map[string]Value
}

func NewRecordValuesTxn(txn Txn) *RecordValuesTxn {
	return &RecordValuesTxn{
		Txn:         txn,
		readValues:  make(map[string]Value),
		writeValues: make(map[string]Value),
	}
}

func (txn *RecordValuesTxn) Get(ctx context.Context, key string, opt TxnReadOption) (Value, error) {
	val, err := txn.Txn.Get(ctx, key, opt)
	if err == nil {
		if txn.GetType().IsSnapshotRead() {
			assert.Must(val.SnapshotVersion == txn.GetSnapshotVersion() && val.Version <= val.SnapshotVersion)
		}
		txn.readValues[key] = val
	}
	return val, err
}

func (txn *RecordValuesTxn) MGet(ctx context.Context, keys []string, opt TxnReadOption) ([]Value, error) {
	values, err := txn.Txn.MGet(ctx, keys, opt)
	if err == nil {
		if txnType, ssVersion := txn.GetType(), txn.GetSnapshotVersion(); txnType.IsSnapshotRead() {
			for _, val := range values {
				assert.Must(val.SnapshotVersion == ssVersion && val.Version <= val.SnapshotVersion)
			}
		}
		for idx, val := range values {
			txn.readValues[keys[idx]] = val
		}
	}
	return values, err
}

func (txn *RecordValuesTxn) Set(ctx context.Context, key string, val []byte) error {
	err := txn.Txn.Set(ctx, key, val)
	if err == nil {
		txn.writeValues[key] = NewValue(val, txn.Txn.GetId().Version())
	}
	return err
}

func (txn *RecordValuesTxn) GetReadValues() map[string]Value {
	return txn.readValues
}

func (txn *RecordValuesTxn) GetWriteValues() map[string]Value {
	return txn.writeValues
}
