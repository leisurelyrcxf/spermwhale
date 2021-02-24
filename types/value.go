package types

type Meta struct {
	WriteIntent bool
	Version     uint64
}

type Value struct {
	Meta

	V string
}

func NewValue(v string, version uint64, writeIntent bool) Value {
	return Value{
		V: v,
		Meta: Meta{
			WriteIntent: writeIntent,
			Version:     version,
		},
	}
}
