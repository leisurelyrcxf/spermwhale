package oracle

import (
	"context"
)

type Oracle interface {
	FetchTimestamp(ctx context.Context) (uint64, error)
	Close() error
}

type Factory interface {
	GetOracle() Oracle
}
