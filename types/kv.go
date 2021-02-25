package types

import "context"

type WriteOption struct {
	Meta
	ClearWriteIntent bool
}

type KV interface {
	Get(ctx context.Context, key string, version uint64) (Value, error)
	Set(ctx context.Context, key, val string, opt WriteOption) error
}
