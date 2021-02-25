package types

type Meta struct {
	Version     uint64
	WriteIntent bool
}

type Value struct {
	Meta

	V []byte
}

func NewValue(val []byte, version uint64, writeIntent bool) Value {
	return Value{
		V: val,
		Meta: Meta{
			WriteIntent: writeIntent,
			Version:     version,
		},
	}
}
