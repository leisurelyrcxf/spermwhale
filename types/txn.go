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
		return "key('" + tk.Key + "')"
	}
	return "txn-record"
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

func (s TxnState) IsStaging() bool {
	return s == TxnStateStaging
}

func (s TxnState) IsCommitted() bool {
	return s == TxnStateCommitted
}

func (s TxnState) IsAborted() bool {
	return s == TxnStateRollbacking || s == TxnStateRollbacked
}

func (s TxnState) isRollbacked() bool {
	return s == TxnStateRollbacked
}

func (s TxnState) IsTerminated() bool {
	return s.IsAborted() || s.IsCommitted()
}

func (s TxnState) AsInt32() int32 {
	return int32(s)
}

func (s TxnState) String() string {
	return stateStrings[s]
}

type AtomicTxnState struct {
	int32
}

func NewAtomicTxnState(state TxnState) AtomicTxnState {
	return AtomicTxnState{int32: state.AsInt32()}
}

func (s *AtomicTxnState) GetTxnState() TxnState {
	return TxnState(atomic.LoadInt32(&s.int32))
}

func (s *AtomicTxnState) SetTxnState(state TxnState) (newState TxnState, terminateOnce bool) {
	for {
		old := atomic.LoadInt32(&s.int32)
		oldState := TxnState(old)
		assert.Must(oldState != TxnStateInvalid)
		assert.Must(!state.IsCommitted() || !oldState.IsAborted())
		assert.Must(!state.IsAborted() || !oldState.IsCommitted())
		assert.Must(!oldState.isRollbacked() || state != TxnStateRollbacking) // rollbacked->rollbacking now allowed
		if atomic.CompareAndSwapInt32(&s.int32, old, state.AsInt32()) {
			return state, !oldState.IsTerminated() && state.IsTerminated()
		}
	}
}

func (s *AtomicTxnState) SetTxnStateUnsafe(state TxnState) (newState TxnState, terminateOnce bool) {
	old := atomic.LoadInt32(&s.int32)
	oldState := TxnState(old)
	assert.Must(oldState != TxnStateInvalid)
	assert.Must(!state.IsCommitted() || !oldState.IsAborted())
	assert.Must(!state.IsAborted() || !oldState.IsCommitted())
	assert.Must(!oldState.isRollbacked() || state != TxnStateRollbacking) // rollbacked->rollbacking now allowed
	atomic.StoreInt32(&s.int32, state.AsInt32())
	return state, !oldState.IsTerminated() && state.IsTerminated()
}

func (s *AtomicTxnState) SetRollbacking() (abortOnce bool) {
	for {
		old := atomic.LoadInt32(&s.int32)
		oldState := TxnState(old)
		assert.Must(oldState != TxnStateInvalid && !oldState.IsCommitted())
		if oldState.IsAborted() {
			return false
		}
		if atomic.CompareAndSwapInt32(&s.int32, old, TxnStateRollbacking.AsInt32()) {
			return true
		}
	}
}

func (s *AtomicTxnState) IsStaging() bool {
	return s.GetTxnState().IsStaging()
}

func (s *AtomicTxnState) IsCommitted() bool {
	return s.GetTxnState().IsCommitted()
}

func (s *AtomicTxnState) IsAborted() bool {
	return s.GetTxnState().IsAborted()
}

func (s *AtomicTxnState) IsTerminated() bool {
	return s.GetTxnState().IsTerminated()
}

func (s *AtomicTxnState) String() string {
	return s.GetTxnState().String()
}

type TxnKind uint8

const (
	TxnKindReadOnly  TxnKind = 1 << iota
	TxnKindReadWrite         // include read for write, read after write, write k1, read k2, etc.
	TxnKindWriteOnly
)

func (k TxnKind) String() string {
	switch k {
	case TxnKindReadOnly:
		return "txn_readonly"
	case TxnKindReadWrite:
		return "txn_read_write"
	case TxnKindWriteOnly:
		return "txn_write_only"
	default:
		panic("unsupported")
	}
}

type TxnType uint8

var (
	basicTxnTypes       []TxnType
	txnTypeDescriptions = map[TxnType]string{}
	desc2TxnType        = map[string]TxnType{}
	BasicTxnTypesDesc   string

	newBasicTxnType = func(flag uint8, desc string) TxnType {
		typ := TxnType(flag)
		basicTxnTypes = append(basicTxnTypes, typ)
		txnTypeDescriptions[typ] = desc
		desc2TxnType[desc] = typ
		desc2TxnType[strconv.Itoa(int(flag))] = typ
		if BasicTxnTypesDesc != "" {
			BasicTxnTypesDesc += "|"
		}
		BasicTxnTypesDesc += fmt.Sprintf("%s(%d)", desc, flag)
		return typ
	}

	TxnTypeDefault           = newBasicTxnType(0, "default")
	TxnTypeReadModifyWrite   = newBasicTxnType(1, "read_modify_write")
	TxnTypeWaitWhenReadDirty = newBasicTxnType(1<<1, "wait_when_read_dirty")
	TxnTypeSnapshotRead      = newBasicTxnType(1<<2, "snapshot_read")
)

func ParseTxnType(str string) (typ TxnType, _ error) {
	parts := strings.Split(str, "|")
	for _, part := range parts {
		mask, ok := desc2TxnType[part]
		if !ok {
			return TxnTypeDefault, errors.ErrInvalidRequest
		}
		typ |= mask
	}
	return typ, nil
}

func (t TxnType) ToUint32() uint32 {
	return uint32(t)
}

func (t TxnType) CondWaitWhenReadDirty(b bool) TxnType {
	if !b {
		return t
	}
	return t | TxnTypeWaitWhenReadDirty
}

func (t TxnType) IsReadModifyWrite() bool {
	return t&TxnTypeReadModifyWrite == TxnTypeReadModifyWrite
}

func (t TxnType) IsWaitWhenReadDirty() bool {
	return t&TxnTypeWaitWhenReadDirty == TxnTypeWaitWhenReadDirty
}

func (t TxnType) IsSnapshotRead() bool {
	return t&TxnTypeSnapshotRead == TxnTypeSnapshotRead
}

func (t TxnType) String() string {
	if t == TxnTypeDefault {
		return "default"
	}
	var ret []string
	for flag, desc := range txnTypeDescriptions {
		if flag != 0 && t&flag == flag {
			ret = append(ret, desc)
		}
	}
	return strings.Join(ret, "|")
}

type TxnSnapshotReadOption struct {
	SnapshotVersion           uint64
	MinAllowedSnapshotVersion uint64
	flag                      uint8
}

func NewTxnSnapshotReadOptionFromPB(opt *txnpb.TxnSnapshotReadOption) TxnSnapshotReadOption {
	return TxnSnapshotReadOption{
		SnapshotVersion:           opt.SnapshotVersion,
		MinAllowedSnapshotVersion: opt.MinAllowedSnapshotVersion,
		flag:                      opt.GetFlagAsUint8(),
	}
}

func (opt TxnSnapshotReadOption) ToPB() *txnpb.TxnSnapshotReadOption {
	return &txnpb.TxnSnapshotReadOption{
		SnapshotVersion:           opt.SnapshotVersion,
		MinAllowedSnapshotVersion: opt.MinAllowedSnapshotVersion,
		Flag:                      uint32(opt.flag),
	}
}

func (opt TxnSnapshotReadOption) Equals(another TxnSnapshotReadOption) bool {
	return opt.SnapshotVersion == another.SnapshotVersion && opt.MinAllowedSnapshotVersion == another.MinAllowedSnapshotVersion && opt.flag == another.flag
}

func (opt TxnSnapshotReadOption) IsExplicitSnapshotVersion() bool {
	return opt.flag&consts.TxnSnapshotReadOptionBitMaskExplicitSnapshotVersion == consts.TxnSnapshotReadOptionBitMaskExplicitSnapshotVersion
}

func (opt TxnSnapshotReadOption) IsRelativeSnapshotVersion() bool {
	return opt.flag&consts.TxnSnapshotReadOptionBitMaskRelativeSnapshotVersion == consts.TxnSnapshotReadOptionBitMaskRelativeSnapshotVersion
}

func (opt TxnSnapshotReadOption) IsRelativeMinAllowedSnapshotVersion() bool {
	return opt.flag&consts.TxnSnapshotReadOptionBitMaskRelativeMinAllowedSnapshotVersion == consts.TxnSnapshotReadOptionBitMaskRelativeMinAllowedSnapshotVersion
}

func (opt TxnSnapshotReadOption) AllowsVersionBack() bool {
	return opt.flag&consts.TxnSnapshotReadOptionBitMaskDontAllowVersionBack == 0
}

func (opt TxnSnapshotReadOption) IsEmpty() bool {
	return opt.SnapshotVersion == 0 && opt.MinAllowedSnapshotVersion == 0 && opt.flag == 0
}

func (opt TxnSnapshotReadOption) WithClearDontAllowsVersionBack() TxnSnapshotReadOption {
	opt.flag &= ^consts.TxnSnapshotReadOptionBitMaskDontAllowVersionBack & 0xff
	assert.Must(opt.AllowsVersionBack()) // TODO remove this
	return opt
}

func (opt *TxnSnapshotReadOption) SetSnapshotVersion(snapshotVersion uint64, checkMinAllowedSnapshotVersion bool) {
	assert.Must(!opt.IsExplicitSnapshotVersion() || opt.SnapshotVersion != 0)
	if (opt.SnapshotVersion == 0 || (opt.AllowsVersionBack() && snapshotVersion < opt.SnapshotVersion)) &&
		(!checkMinAllowedSnapshotVersion || snapshotVersion >= opt.MinAllowedSnapshotVersion) {
		opt.SnapshotVersion = snapshotVersion
	}
}

func (opt TxnSnapshotReadOption) String() string {
	return fmt.Sprintf("snapshotVersion: %d, MinAllowedSnapshotVersion: %d, "+
		"explicit_snapshot_version: %v, relative_snapshot_version: %v, "+
		"relative_min_allowed_snapshot_version: %v, allows_version_back: %v",
		opt.SnapshotVersion, opt.MinAllowedSnapshotVersion,
		opt.IsExplicitSnapshotVersion(), opt.IsRelativeSnapshotVersion(),
		opt.IsRelativeMinAllowedSnapshotVersion(), opt.AllowsVersionBack())
}

type TxnOption struct {
	TxnType

	SnapshotReadOption TxnSnapshotReadOption // only valid if TxnType is SnapshotRead
}

func NewDefaultTxnOption() TxnOption {
	return TxnOption{TxnType: TxnTypeDefault}
}

func NewTxnOption(typ TxnType) TxnOption {
	return TxnOption{TxnType: typ}
}

func NewTxnOptionFromPB(option *txnpb.TxnOption) TxnOption {
	return TxnOption{
		TxnType:            TxnType(option.GetTxnType()),
		SnapshotReadOption: NewTxnSnapshotReadOptionFromPB(option.SnapshotReadOption),
	}
}

func (opt TxnOption) ToPB() *txnpb.TxnOption {
	return &txnpb.TxnOption{
		Type:               opt.TxnType.ToUint32(),
		SnapshotReadOption: opt.SnapshotReadOption.ToPB(),
	}
}

func (opt TxnOption) WithSnapshotVersion(snapshotVersion uint64) TxnOption {
	assert.Must(opt.IsSnapshotRead())
	opt.SnapshotReadOption.SnapshotVersion = snapshotVersion
	opt.SnapshotReadOption.flag |= consts.TxnSnapshotReadOptionBitMaskExplicitSnapshotVersion
	opt.SnapshotReadOption.flag |= consts.TxnSnapshotReadOptionBitMaskDontAllowVersionBack
	return opt
}

func (opt TxnOption) WithRelativeSnapshotVersion(snapshotVersionDiff uint64) TxnOption {
	assert.Must(opt.IsSnapshotRead())
	opt.SnapshotReadOption.SnapshotVersion = snapshotVersionDiff
	opt.SnapshotReadOption.flag |= consts.TxnSnapshotReadOptionBitMaskExplicitSnapshotVersion
	opt.SnapshotReadOption.flag |= consts.TxnSnapshotReadOptionBitMaskDontAllowVersionBack
	opt.SnapshotReadOption.flag |= consts.TxnSnapshotReadOptionBitMaskRelativeSnapshotVersion
	return opt
}

func (opt TxnOption) WithSnapshotReadMinAllowedSnapshotVersion(minAllowedSnapshotVersion uint64) TxnOption {
	assert.Must(opt.IsSnapshotRead())
	opt.SnapshotReadOption.MinAllowedSnapshotVersion = minAllowedSnapshotVersion
	return opt
}

func (opt TxnOption) WithSnapshotReadRelativeMinAllowedSnapshotVersion(relativeMinAllowedSnapshotVersionDiff uint64) TxnOption {
	assert.Must(opt.IsSnapshotRead())
	opt.SnapshotReadOption.MinAllowedSnapshotVersion = relativeMinAllowedSnapshotVersionDiff
	opt.SnapshotReadOption.flag |= consts.TxnSnapshotReadOptionBitMaskRelativeMinAllowedSnapshotVersion
	return opt
}

func (opt TxnOption) WithSnapshotReadDontAllowVersionBack() TxnOption {
	assert.Must(opt.IsSnapshotRead())
	opt.SnapshotReadOption.flag |= consts.TxnSnapshotReadOptionBitMaskDontAllowVersionBack
	return opt
}

func (opt TxnOption) CondSnapshotReadDontAllowVersionBack(b bool) TxnOption {
	if !opt.IsSnapshotRead() {
		return opt
	}
	if b {
		opt.SnapshotReadOption.flag |= consts.TxnSnapshotReadOptionBitMaskDontAllowVersionBack
	}
	return opt
}

type TxnManager interface {
	BeginTransaction(ctx context.Context, opt TxnOption) (Txn, error)
	Close() error
}

func ValidateMGetRequest(keys []string) error {
	if len(keys) == 0 {
		return errors.ErrEmptyKeys
	}
	for _, key := range keys {
		if key == "" {
			return errors.ErrEmptyKey
		}
	}
	return nil
}

func ValidateMSetRequest(keys []string, values [][]byte) error {
	if err := ValidateMGetRequest(keys); err != nil {
		return err
	}
	if len(keys) != len(values) {
		return errors.Annotatef(errors.ErrInvalidRequest, "len(keys) != len(values)")
	}
	return nil
}

type Txn interface {
	GetId() TxnId
	GetState() TxnState
	GetType() TxnType
	GetSnapshotReadOption() TxnSnapshotReadOption // only used when txn type is snapshot
	Get(ctx context.Context, key string) (Value, error)
	MGet(ctx context.Context, keys []string) (values []Value, err error)
	Set(ctx context.Context, key string, val []byte) error // async func, doesn't guarantee see set result after call
	MSet(ctx context.Context, keys []string, values [][]byte) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error

	GetReadValues() map[string]Value
	GetWriteValues() map[string]Value
}

type RecordValuesTxn struct {
	Txn
	readValues, writeValues ReadResult
}

func NewRecordValuesTxn(txn Txn) *RecordValuesTxn {
	return &RecordValuesTxn{
		Txn:         txn,
		readValues:  make(map[string]Value),
		writeValues: make(map[string]Value),
	}
}

func (txn *RecordValuesTxn) Get(ctx context.Context, key string) (Value, error) {
	val, err := txn.Txn.Get(ctx, key)
	if err == nil {
		assert.Must(!val.IsDirty() || (val.Version == txn.GetId().Version() && txn.writeValues.Contains(key)))
		if txn.GetType().IsSnapshotRead() {
			assert.Must(val.SnapshotVersion == txn.GetSnapshotReadOption().SnapshotVersion && val.Version <= val.SnapshotVersion)
		}
		txn.readValues[key] = val
	}
	return val, err
}

func (txn *RecordValuesTxn) MGet(ctx context.Context, keys []string) ([]Value, error) {
	values, err := txn.Txn.MGet(ctx, keys)
	if err == nil {
		for idx, val := range values {
			assert.Must(!val.IsDirty() || (val.Version == txn.GetId().Version() && txn.writeValues.Contains(keys[idx])))
		}
		if txnType, ssVersion := txn.GetType(), txn.GetSnapshotReadOption().SnapshotVersion; txnType.IsSnapshotRead() {
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
		txn.writeValues[key] = NewValue(val, txn.Txn.GetId().Version()).
			WithNoWriteIntent().WithInternalVersion(txn.writeValues[key].InternalVersion + 1)
	}
	return err
}

func (txn *RecordValuesTxn) MSet(ctx context.Context, keys []string, values [][]byte) error {
	err := txn.Txn.MSet(ctx, keys, values)
	if err == nil {
		for idx, key := range keys {
			txn.writeValues[key] = NewValue(values[idx], txn.Txn.GetId().Version()).
				WithNoWriteIntent().WithInternalVersion(txn.writeValues[key].InternalVersion + 1)
		}
	}
	return err
}

func (txn *RecordValuesTxn) GetReadValues() map[string]Value {
	return txn.readValues
}

func (txn *RecordValuesTxn) GetWriteValues() map[string]Value {
	return txn.writeValues
}

var (
	InvalidReadValues   = map[string]Value{"haha": {Meta: Meta{Version: 1111}}}
	IsInvalidReadValues = func(values map[string]Value) bool { return values["haha"].Version == 1111 }

	InvalidWriteValues   = map[string]Value{"biubiu": {Meta: Meta{Version: 1111}}}
	IsInvalidWriteValues = func(values map[string]Value) bool { return values["biubiu"].Version == 1111 }
)
