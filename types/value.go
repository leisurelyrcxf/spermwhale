package types

import (
	"fmt"
	"strconv"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"
	"github.com/leisurelyrcxf/spermwhale/proto/txnpb"
)

type VFlag uint8

func (m VFlag) GetKeyState() KeyState {
	return KeyState(consts.ExtractTxnBits(uint8(m)))
}

func (m VFlag) IsValid() bool {
	return m != 0 && m&consts.TxnStateBitMaskInvalid == 0 &&
		(!m.IsClearedUnsafe() || m.IsTerminated()) &&
		(!m.IsCommitted() || (!m.IsDirty() && !m.IsAborted())) &&
		(!m.IsAborted() || (m.IsDirty() && !m.IsCommitted()))
}

func (m VFlag) AssertValid() {
	assert.Must(m != 0 && m&consts.TxnStateBitMaskInvalid == 0)
	assert.Must(!m.IsClearedUnsafe() || m.IsTerminated())
	assert.Must(!m.IsCommitted() || (!m.IsDirty() && !m.IsAborted()))
	assert.Must(!m.IsAborted() || (m.IsDirty() && !m.IsCommitted()))
}

func (m VFlag) IsDirty() bool {
	return m&consts.ValueMetaBitMaskHasWriteIntent == consts.ValueMetaBitMaskHasWriteIntent
}

func (m VFlag) IsCommitted() bool {
	return consts.IsCommitted(uint8(m))
}

func (m VFlag) IsUncommitted() bool {
	return consts.IsUncommittedValue(uint8(m))
}

func (m VFlag) IsAborted() bool {
	return consts.IsAborted(uint8(m))
}

func (m VFlag) IsRollbacking() bool {
	return consts.IsRollbacking(uint8(m))
}

func (m VFlag) IsTerminated() bool {
	return consts.IsTerminated(uint8(m))
}

func (m VFlag) IsCleared() bool {
	cleared := consts.IsCleared(uint8(m))
	assert.Must(!cleared || m.IsTerminated()) // TODO remove this in product
	return cleared
}

func (m VFlag) IsClearedUnsafe() bool {
	return consts.IsCleared(uint8(m))
}

func (m VFlag) IsClearedUint() uint8 {
	return consts.IsClearedUint(uint8(m))
}

func (m VFlag) IsCommittedCleared() bool {
	return consts.IsCommittedClearedValue(uint8(m))
}

func (m VFlag) IsRollbackedCleared() bool {
	return consts.IsRollbackedClearedValue(uint8(m))
}

func (m VFlag) IsKeyStateInvalid() bool {
	return m&consts.ValueMetaBitMaskInvalidKeyState == consts.ValueMetaBitMaskInvalidKeyState
}

func (m VFlag) IsTxnRecord() bool {
	return m&consts.ValueMetaBitMaskTxnRecord == consts.ValueMetaBitMaskTxnRecord
}

func (m VFlag) IsFutureWritePrevented() bool {
	return m&consts.ValueMetaBitMaskPreventedFutureWrite == consts.ValueMetaBitMaskPreventedFutureWrite
}

func (m VFlag) String() string {
	return fmt.Sprintf("has_write_intent: %v, committed: %v, aborted: %v, cleared: %v, invalid: %v, txn_record: %v, prevent_future_write: %v",
		m.IsDirty(), m.IsCommitted(), m.IsAborted(), m.IsCleared(), m.IsKeyStateInvalid(), m.IsTxnRecord(), m.IsFutureWritePrevented())
}

func (m *VFlag) SetCommitted() {
	*m &= consts.ValueMetaBitMaskClearWriteIntent
	*m |= consts.ValueMetaBitMaskCommitted
}

func (m *VFlag) SetAborted() {
	*m |= consts.ValueMetaBitMaskAborted
}

func (m *VFlag) SetCommittedCleared() {
	*m &= consts.ValueMetaBitMaskClearWriteIntent
	*m |= consts.ValueMetaBitMaskCommitted | consts.ValueMetaBitMaskCleared
}

func (m *VFlag) SetCleared() {
	*m |= consts.ValueMetaBitMaskCleared
}

func (m *VFlag) SetInvalidKeyState() {
	*m |= consts.ValueMetaBitMaskInvalidKeyState
}

func (m *VFlag) ClearInvalidKeyState() {
	*m &= consts.ValueMetaBitMaskClearInvalidKeyState
}

func (m *VFlag) UpdateKeyState(state KeyState) {
	if state.IsCommitted() {
		*m &= consts.ValueMetaBitMaskClearWriteIntent
	}
	m.UpdateAbortedKeyState(state)
}

// UpdateAbortedTxnState only use this when you known what you are doing
func (m *VFlag) UpdateAbortedKeyState(abortedState KeyState) {
	*m |= abortedState.ToVFlag()
}

func (m *VFlag) UpdateCommittedTxnState(committedState TxnState) {
	*m &= consts.ValueMetaBitMaskClearWriteIntent
	*m |= committedState.ToVFlag()
}

// UpdateAbortedTxnState only use this when you known what you are doing
func (m *VFlag) UpdateAbortedTxnState(abortedState TxnState) {
	*m |= abortedState.ToVFlag()
}

func (m *VFlag) UpdateTxnState(state TxnState) {
	if state.IsCommitted() {
		*m &= consts.ValueMetaBitMaskClearWriteIntent
	}
	*m |= state.ToVFlag()
}

type Meta struct {
	Version         uint64
	InternalVersion TxnInternalVersion
	VFlag
}

func (m Meta) IsValid() bool {
	return m.Version != 0 && m.InternalVersion.IsValid() && m.VFlag.IsValid()
}

func (m Meta) AssertValid() {
	assert.Must(m.Version != 0)
	assert.Must(m.InternalVersion.IsValid())
	m.VFlag.AssertValid()
}

func (m Meta) IsEmpty() bool {
	return m.Version == 0
}

func (m Meta) IsFirstWrite() bool {
	return m.InternalVersion == TxnInternalVersionMin
}

func NewMetaFromPB(x *commonpb.ValueMeta) Meta {
	return Meta{
		Version:         x.Version,
		InternalVersion: TxnInternalVersion(x.InternalVersion),
		VFlag:           VFlag(x.GetFlagSafe()),
	}
}

func (m Meta) ToPB() *commonpb.ValueMeta {
	return (&commonpb.ValueMeta{
		Version:         m.Version,
		InternalVersion: uint32(m.InternalVersion),
	}).SetFlag(uint8(m.VFlag))
}

func (m Meta) ToDB() DBMeta {
	return DBMeta{
		InternalVersion: m.InternalVersion,
		VFlag:           m.VFlag,
	}
}

type Value struct {
	Meta

	V []byte
}

var EmptyValue = Value{}

// NewValue create a value with write intent
func NewValue(val []byte, version uint64) Value {
	return Value{
		Meta: Meta{
			Version: version,
			VFlag:   consts.ValueMetaBitMaskHasWriteIntent,
		},
		V: val,
	}
}
func NewTxnValue(val []byte, version uint64) Value {
	return Value{
		Meta: Meta{
			Version: version,
			VFlag:   consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskTxnRecord,
		},
		V: val,
	}
}
func NewIntValue(i int) Value {
	return NewValue([]byte(strconv.Itoa(i)), 0) // TODO change coding
}
func NewValueFromPB(x *commonpb.Value) Value {
	return Value{
		Meta: NewMetaFromPB(x.Meta),
		V:    x.V,
	}
}

func (v Value) ToPB() *commonpb.Value {
	return &commonpb.Value{
		Meta: v.Meta.ToPB(),
		V:    v.V,
	}
}
func (v Value) ToDB() DBValue {
	return DBValue{
		DBMeta: v.Meta.ToDB(),
		V:      v.V,
	}
}

func (v Value) IsValid() bool {
	return v.Meta.IsValid() && len(v.V) > 0
}

func (v Value) AssertValid() {
	v.Meta.AssertValid()
	assert.Must(len(v.V) > 0)
}

func (v Value) String() string {
	return string(v.V)
}

func (v Value) Int() (int, error) {
	x, err := strconv.ParseInt(string(v.V), 10, 64)
	return int(x), err
}

func (v Value) MustInt() int {
	x, err := strconv.ParseInt(string(v.V), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("invalid int value '%s'", string(v.V)))
	}
	return int(x)
}

func (v Value) IsEmpty() bool {
	return len(v.V) == 0 && v.Meta.IsEmpty()
}

func (v Value) WithVersion(version uint64) Value {
	v.Version = version
	return v
}

func (v Value) WithCommitted() Value {
	v.SetCommitted()
	return v
}

func (v Value) WithInternalVersion(version TxnInternalVersion) Value {
	v.Meta.InternalVersion = version
	return v
}

func (v Value) WithMaxReadVersion(maxReadVersion uint64) ValueCC {
	return ValueCC{
		Value:          v,
		MaxReadVersion: maxReadVersion,
	}
}

func (v Value) WithSnapshotVersion(snapshotVersion uint64) ValueCC {
	return ValueCC{
		Value:           v,
		SnapshotVersion: snapshotVersion,
	}
}

type ValueCC struct {
	Value

	MaxReadVersion  uint64
	SnapshotVersion uint64
}

var EmptyValueCC = ValueCC{}

func NewValueCCFromPB(x *commonpb.ValueCC) ValueCC {
	if x.Value == nil {
		return ValueCC{
			MaxReadVersion:  x.MaxReadVersion,
			SnapshotVersion: x.SnapshotVersion,
		}
	}
	return ValueCC{
		Value:           NewValueFromPB(x.Value),
		MaxReadVersion:  x.MaxReadVersion,
		SnapshotVersion: x.SnapshotVersion,
	}
}

func (v ValueCC) ToPB() *commonpb.ValueCC {
	return &commonpb.ValueCC{
		Value:           v.Value.ToPB(),
		MaxReadVersion:  v.MaxReadVersion,
		SnapshotVersion: v.SnapshotVersion,
	}
}

func (v ValueCC) IsEmpty() bool {
	return v.Value.IsEmpty() && v.MaxReadVersion == 0 && v.SnapshotVersion == 0
}

// Hide Value::WithMaxReadVersion
func (v ValueCC) WithMaxReadVersion() ValueCC {
	panic(errors.ErrNotSupported)
}

// Hide Value::WithMaxReadVersion
func (v ValueCC) WithSnapshotVersion() ValueCC {
	panic(errors.ErrNotSupported)
}

func (v ValueCC) ToTValue() TValue {
	return TValue{
		Value:           v.Value,
		SnapshotVersion: v.SnapshotVersion,
	}
}

type ValueCCs []ValueCC

func (vs ValueCCs) ToValues() []Value {
	ret := make([]Value, 0, len(vs))
	for _, v := range vs {
		ret = append(ret, v.Value)
	}
	return ret
}

type ReadResultCC map[string]ValueCC

func (r ReadResultCC) MustFirst() string {
	for key := range r {
		return key
	}
	panic("empty ReadResultCC")
}

func (r ReadResultCC) Contains(key string) bool {
	_, ok := r[key]
	return ok
}

func (r ReadResultCC) ToTValues(keys []string, newSnapshotVersion uint64) []TValue {
	ret := make([]TValue, len(keys))
	for idx, key := range keys {
		assert.Must(r.Contains(key))
		ret[idx] = NewTValue(r[key].Value, newSnapshotVersion)
	}
	return ret
}

var EmptyTValue = TValue{}

type TValue struct {
	Value
	SnapshotVersion uint64
}

func NewTValue(value Value, snapshotVersion uint64) TValue {
	return TValue{
		Value:           value,
		SnapshotVersion: snapshotVersion,
	}
}

func NewTValueFromPB(x *txnpb.TValue) TValue {
	if x.Value == nil {
		return TValue{
			SnapshotVersion: x.SnapshotVersion,
		}
	}
	return TValue{
		Value:           NewValueFromPB(x.Value),
		SnapshotVersion: x.SnapshotVersion,
	}
}

func (v TValue) ToPB() *txnpb.TValue {
	return &txnpb.TValue{
		Value:           v.Value.ToPB(),
		SnapshotVersion: v.SnapshotVersion,
	}
}

func (v TValue) IsEmpty() bool {
	return v.Value.IsEmpty() && v.SnapshotVersion == 0
}

func (v TValue) CondPreventedFutureWrite(b bool) TValue {
	if b {
		v.VFlag |= consts.ValueMetaBitMaskPreventedFutureWrite
	}
	return v
}

type TValues []TValue

func (vs TValues) ToPB() []*txnpb.TValue {
	if len(vs) == 0 {
		return nil
	}
	pbValues := make([]*txnpb.TValue, len(vs))
	for idx, v := range vs {
		pbValues[idx] = v.ToPB()
	}
	return pbValues
}

func NewTValuesFromPB(pbValues []*txnpb.TValue) []TValue {
	if len(pbValues) == 0 {
		return nil
	}
	ret := make([]TValue, len(pbValues))
	for idx, pbVal := range pbValues {
		ret[idx] = NewTValueFromPB(pbVal)
	}
	return ret
}

func init() {
	val, clearedVal := NewValue(nil, 123), NewValue(nil, 123)
	clearedVal.SetCleared()

	assert.Must(errors.GetReadUncommittedDataOfAbortedTxn(val.IsClearedUint()).SubCode == consts.ErrSubCodeReadUncommittedDataPrevTxnRollbacking)
	assert.Must(errors.GetReadUncommittedDataOfAbortedTxn(clearedVal.IsClearedUint()).SubCode == consts.ErrSubCodeReadUncommittedDataPrevTxnRollbacked)

	assert.Must(errors.GetNotExistsErrForAborted(val.IsClearedUint()).SubCode == consts.ErrSubCodeKeyOrVersionNotExistsExistsInDBButRollbacking)
	assert.Must(errors.GetNotExistsErrForAborted(clearedVal.IsClearedUint()).SubCode == consts.ErrSubCodeKeyOrVersionNotExistsInDB)
}
