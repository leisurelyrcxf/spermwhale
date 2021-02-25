package types

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
