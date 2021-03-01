package types

import (
	"fmt"
	"strconv"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"
)

type Meta struct {
	Version uint64
	Flag    uint8
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

func (m Meta) IsEmpty() bool {
	return m.Version == 0 && !m.IsMaxReadVersionBiggerThanRequested()
}

func (m Meta) HasWriteIntent() bool {
	return m.Flag&consts.ValueMetaBitMaskHasWriteIntent > 0
}

func (m Meta) IsMaxReadVersionBiggerThanRequested() bool {
	return m.Flag&consts.ValueMetaBitMaskMaxReadVersionBiggerThanRequested > 0
}

func (m *Meta) ClearWriteIntent() {
	m.Flag &= 0xfe
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
		V:    x.Val,
		Meta: NewMetaFromPB(x.Meta),
	}
}

func (v Value) ToPB() *commonpb.Value {
	if v.IsEmpty() {
		return nil
	}
	return &commonpb.Value{
		Meta: v.Meta.ToPB(),
		Val:  v.V,
	}
}

func (v Value) IsEmpty() bool {
	return v.Meta.IsEmpty() && len(v.V) == 0
}

func (v Value) WithVersion(version uint64) Value {
	v.Version = version
	return v
}

func (v Value) WithNoWriteIntent() Value {
	v.ClearWriteIntent()
	return v
}

func (v Value) WithMaxReadVersionBiggerThanRequested() Value {
	v.Flag |= consts.ValueMetaBitMaskMaxReadVersionBiggerThanRequested
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

func IntValue(i int) Value {
	return NewValue([]byte(strconv.Itoa(i)), 0)
}
