package types

import "context"

type KV interface {
	Get(ctx context.Context, key string, version uint64) (VersionedValue, error)
	Set(ctx context.Context, key, val string, version uint64, writeIntent bool) error
}
