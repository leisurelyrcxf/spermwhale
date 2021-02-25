package types

import "context"

type ReadOption struct {
	Version                 uint64
	ExactVersion            bool
	NotUpdateTimestampCache bool
}

func NewReadOption(version uint64) ReadOption {
	return ReadOption{
		Version: version,
	}
}

func (opt ReadOption) SetExactVersion() ReadOption {
	opt.ExactVersion = true
	return opt.SetNotUpdateTimestampCache()
}

func (opt ReadOption) SetNotUpdateTimestampCache() ReadOption {
	opt.NotUpdateTimestampCache = true
	return opt
}

type WriteOption struct {
	ClearWriteIntent bool
	RemoveVersion    bool
}

func NewWriteOption() WriteOption {
	return WriteOption{}
}

func (opt WriteOption) SetClearWriteIntent() WriteOption {
	opt.ClearWriteIntent = true
	return opt
}

func (opt WriteOption) SetRemoveVersion() WriteOption {
	opt.RemoveVersion = true
	return opt
}

type KV interface {
	Get(ctx context.Context, key string, opt ReadOption) (Value, error)
	Set(ctx context.Context, key string, val Value, opt WriteOption) error
	Close() error
}

type Txn interface {
	Begin(ctx context.Context) (uint64, error)
	Get(ctx context.Context, key string, txnID uint64) (Value, error)
	Set(ctx context.Context, key string, val []byte, txnID uint64) error
	Commit(ctx context.Context, txnID uint64) error
	Rollback(ctx context.Context, txnID uint64) error
	Close() error
}
