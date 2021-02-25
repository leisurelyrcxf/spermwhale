package types

import "context"

type ReadOption struct {
	Version      uint64
	ExactVersion bool
}

func NewReadOption(version uint64, exactVersion bool) ReadOption {
	return ReadOption{
		Version:      version,
		ExactVersion: exactVersion,
	}
}

type WriteOption struct {
	ClearWriteIntent bool
	RemoveVersion    bool
}

type KV interface {
	Get(ctx context.Context, key string, opt ReadOption) (Value, error)
	Set(ctx context.Context, key string, val Value, opt WriteOption) error
}
