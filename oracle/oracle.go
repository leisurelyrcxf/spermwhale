package oracle

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/errors"
)

type Oracle interface {
	FetchTimestamp(ctx context.Context) (uint64, error)
	Close() error
}

type DummyOracle struct{}

func (o DummyOracle) FetchTimestamp(_ context.Context) (uint64, error) {
	return 0, errors.Annotatef(errors.ErrNotSupported, "by DummyOracle")
}
func (o DummyOracle) Close() error {
	return errors.Annotatef(errors.ErrNotSupported, "by DummyOracle")
}

type Factory interface {
	GetOracle() Oracle
}
