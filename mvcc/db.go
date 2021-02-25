package mvcc

import (
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/types"
)

var (
	ErrKeyNotExist     = fmt.Errorf("key not exist")
	ErrVersionTooStale = fmt.Errorf("version too stale")
)

type DB interface {
	Get(key string, upperVersion uint64) (types.Value, error)
	Set(key string, val string, version uint64, writeIntent bool)
}
