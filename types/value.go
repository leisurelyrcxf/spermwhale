package types

import (
	"fmt"
	"strconv"

	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"
)

type Meta struct {
	Version     uint64
	WriteIntent bool
}

func NewMetaFromPB(x *commonpb.ValueMeta) Meta {
	return Meta{
		Version:     x.Version,
		WriteIntent: x.WriteIntent,
	}
}

func (m Meta) ToPB() *commonpb.ValueMeta {
	return &commonpb.ValueMeta{
		Version:     m.Version,
		WriteIntent: m.WriteIntent,
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
		V: val,
		Meta: Meta{
			WriteIntent: true,
			Version:     version,
		},
	}
}

func NewValueFromPB(x *commonpb.Value) Value {
	return Value{
		V:    x.Val,
		Meta: NewMetaFromPB(x.Meta),
	}
}

func (v Value) SetVersion(version uint64) Value {
	v.Version = version
	return v
}

func (v Value) SetNoWriteIntent() Value {
	v.WriteIntent = false
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

func (v Value) ToPB() *commonpb.Value {
	return &commonpb.Value{
		Meta: v.Meta.ToPB(),
		Val:  v.V,
	}
}

func IntValue(i int) Value {
	return NewValue([]byte(strconv.Itoa(i)), 0)
}
