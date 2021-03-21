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
}

func NewMetaFromPB(x *commonpb.ValueMeta) Meta {
	return Meta{
		Version: x.Version,
		Flag:    x.GetFlagSafe(),
	}
}

func (m Meta) ToPB() *commonpb.ValueMeta {
	return (&commonpb.ValueMeta{
		Version: m.Version,
	}).SetFlag(m.Flag)
}

func (m Meta) isEmpty() bool {
	return m.Version == 0
}

func (m Meta) HasWriteIntent() bool {
	return m.Flag&consts.ValueMetaBitMaskHasWriteIntent == consts.ValueMetaBitMaskHasWriteIntent
}

func (m Meta) IsFirstWriteOfKey() bool {
	return m.InternalVersion == consts.MinTxnInternalVersion
}

type Value struct {
	Meta

	V []byte
}

var EmptyValue = Value{}

// NewValue create a value with write intent
func NewValue(val []byte, version uint64) Value {
	return Value{
		V: val,
		Meta: Meta{
			Version: version,
			Flag:    consts.ValueMetaBitMaskHasWriteIntent, // default has write intent
		},
	}
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

func (v Value) IsEmpty() bool {
	return len(v.V) == 0 && v.Meta.isEmpty()
}

func (v Value) WithVersion(version uint64) Value {
	v.Version = version
	return v
}

func (v Value) WithNoWriteIntent() Value {
	v.Flag &= 0xfe
	return v
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

func IntValue(i int) Value {
	return NewValue([]byte(strconv.Itoa(i)), 0)
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
	return v.Value.IsEmpty() && v.MaxReadVersion == 0
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
