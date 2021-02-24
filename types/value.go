package types

type Meta struct {
	WriteIntent bool
}

type Value struct {
	M Meta

	V string
}

func NewValue(v string, writeIntent bool) Value {
	return Value{
		V: v,
		M: Meta{WriteIntent: writeIntent},
	}
}

type VersionedValue struct {
	Value
	Version uint64
}

func NewVersionedValue(val Value, version uint64) VersionedValue {
	return VersionedValue{
		Value:   val,
		Version: version,
	}
}
