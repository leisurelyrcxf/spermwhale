package types

type VersionedValue struct {
    Value   string
    Version uint64
}

func NewVersionedValue(val string, version uint64) VersionedValue {
    return VersionedValue{
        Value:   val,
        Version: version,
    }
}
