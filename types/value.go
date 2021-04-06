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

type Meta struct {
	Version         uint64
	InternalVersion TxnInternalVersion
	Flag            uint8
}

func NewMetaFromPB(x *commonpb.ValueMeta) Meta {
	return Meta{
		Version:         x.Version,
		InternalVersion: TxnInternalVersion(x.InternalVersion),
		Flag:            x.GetFlagSafe(),
	}
}

func (m Meta) ToPB() *commonpb.ValueMeta {
	return (&commonpb.ValueMeta{
		Version:         m.Version,
		InternalVersion: uint32(m.InternalVersion),
	}).SetFlag(m.Flag)
}

func (m Meta) ToDB() DBMeta {
	return DBMeta{
		InternalVersion: m.InternalVersion,
		Flag:            m.Flag,
	}
}

func (m *Meta) SetCommitted() {
	m.Flag = consts.WithCommitted(m.Flag)
}

func (m *Meta) SetAborted() {
	m.Flag |= consts.ValueMetaBitMaskAborted
}

func (m Meta) IsEmpty() bool {
	return m.Version == 0
}

func (m Meta) IsFirstWriteOfKey() bool {
	return m.InternalVersion == TxnInternalVersionMin // For txn record, InternalVersion is always 0
}

func (m Meta) IsWriteOfKey() bool {
	return m.InternalVersion >= TxnInternalVersionMin // For txn record, InternalVersion is always 0
}

func (m Meta) IsDirty() bool {
	return consts.IsDirty(m.Flag)
}

func (m Meta) IsCommitted() bool {
	return m.Flag&consts.ValueMetaBitMaskCommitted == consts.ValueMetaBitMaskCommitted
}

func (m Meta) IsTerminated() bool {
	return m.Flag&consts.ValueMetaBitMaskTerminated != 0
}

func (m Meta) IsAborted() bool {
	return m.Flag&consts.ValueMetaBitMaskAborted == consts.ValueMetaBitMaskAborted
}

func (m *Meta) Update(state TxnState) (isAborted bool) {
	if state.IsAborted() {
		m.SetAborted()
		return true
	}
	if state.IsCommitted() {
		m.SetCommitted()
		return false
	}
	return false
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
			Flag:    consts.ValueMetaBitMaskHasWriteIntent,
		},
		V: val,
	}
}

func NewValueFromPB(x *commonpb.Value) Value {
	return Value{
		Meta: NewMetaFromPB(x.Meta),
		V:    x.V,
	}
}

func NewIntValue(i int) Value {
	return NewValue([]byte(strconv.Itoa(i)), 0) // TODO change coding
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
func (v ValueCC) WithMaxReadVersion(maxReadVersion uint64) ValueCC {
	panic(errors.ErrNotSupported)
	return v
}

// Hide Value::WithMaxReadVersion
func (v ValueCC) WithSnapshotVersion(ssVersion uint64) ValueCC {
	panic(errors.ErrNotSupported)
	return v
}

func (v ValueCC) WithCommitted() ValueCC {
	v.SetCommitted()
	return v
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
		v.Flag |= consts.ValueMetaBitMaskPreventedFutureWrite
	}
	return v
}

func (v TValue) IsFutureWritePrevented() bool {
	return v.Flag&consts.ValueMetaBitMaskPreventedFutureWrite == consts.ValueMetaBitMaskPreventedFutureWrite
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
