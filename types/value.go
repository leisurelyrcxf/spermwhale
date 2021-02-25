package types

import (
	"fmt"
	"strconv"
)

type Meta struct {
	Version     uint64
	WriteIntent bool
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

func (v Value) SetNoWriteIntent() Value {
	v.WriteIntent = false
	return v
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
