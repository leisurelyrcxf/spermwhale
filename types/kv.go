package types

import "context"

type WriteOption struct {
	ClearWriteIntent bool
}

type KV interface {
	Get(ctx context.Context, key string, version uint64) (Value, error)
	Set(ctx context.Context, key string, val Value, opt WriteOption) error
}
