package types

import (
	"fmt"
	"strconv"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"
)

type Meta struct {
	Version         uint64
	InternalVersion TxnInternalVersion
	Flag            uint8
	SnapshotVersion uint64 // used only for snapshot read
}

func NewMetaFromPB(x *commonpb.ValueMeta) Meta {
	return Meta{
		Version:         x.Version,
		InternalVersion: TxnInternalVersion(x.InternalVersion),
		Flag:            x.GetFlagSafe(),
		SnapshotVersion: x.SnapshotVersion,
	}
}

func (m Meta) ToPB() *commonpb.ValueMeta {
	return (&commonpb.ValueMeta{
		Version:         m.Version,
		InternalVersion: uint32(m.InternalVersion),
		SnapshotVersion: m.SnapshotVersion,
	}).SetFlag(m.Flag)
}

func (m Meta) isEmpty() bool {
	return m.Version == 0
}

func (m Meta) HasWriteIntent() bool {
	return m.Flag&consts.ValueMetaBitMaskHasWriteIntent == consts.ValueMetaBitMaskHasWriteIntent
}

func (m Meta) IsFirstWriteOfKey() bool {
	return m.InternalVersion == TxnInternalVersionMin
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
			Flag:    consts.ValueMetaBitMaskHasWriteIntent, // default has write intent
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
	return len(v.V) == 0 && v.Meta.isEmpty()
}

func (v Value) WithVersion(version uint64) Value {
	v.Version = version
	return v
}

func (v Value) WithNoWriteIntent() Value {
	v.Flag &= consts.ValueMetaBitMaskClearWriteIntent
	return v
}

func (v Value) WithMaxReadVersion(maxReadVersion uint64) ValueCC {
	return ValueCC{
		Value:          v,
		MaxReadVersion: maxReadVersion,
	}
}

func (v Value) WithInternalVersion(version TxnInternalVersion) Value {
	v.Meta.InternalVersion = version
	return v
}

func (v Value) WithSnapshotVersion(snapshotVersion uint64) Value {
	v.SnapshotVersion = snapshotVersion
	return v
}

type ValueCC struct {
	Value

	MaxReadVersion uint64
}

var EmptyValueCC = ValueCC{}

func NewValueCCFromPB(x *commonpb.ValueCC) ValueCC {
	if x.Value == nil {
		return ValueCC{
			MaxReadVersion: x.MaxReadVersion,
		}
	}
	return ValueCC{
		Value:          NewValueFromPB(x.Value),
		MaxReadVersion: x.MaxReadVersion,
	}
}

func (v ValueCC) ToPB() *commonpb.ValueCC {
	if v.IsEmpty() {
		return nil
	}
	return &commonpb.ValueCC{
		Value:          v.Value.ToPB(),
		MaxReadVersion: v.MaxReadVersion,
	}
}

func (v ValueCC) IsEmpty() bool {
	return v.Value.IsEmpty() && v.MaxReadVersion == 0 && v.SnapshotVersion == 0
}

func (v ValueCC) WithMaxReadVersion(maxReadVersion uint64) ValueCC {
	v.MaxReadVersion = maxReadVersion
	return v
}

func (v ValueCC) WithNoWriteIntent() ValueCC {
	return ValueCC{
		Value:          v.Value.WithNoWriteIntent(),
		MaxReadVersion: v.MaxReadVersion,
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

func (r ReadResultCC) ToValues(keys []string, newSnapshotVersion uint64) []Value {
	ret := make([]Value, 0, len(keys))
	for _, key := range keys {
		val := r[key].Value
		val.SnapshotVersion = newSnapshotVersion
		ret = append(ret, val)
	}
	return ret
}
