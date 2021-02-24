package mvcc

import "github.com/leisurelyrcxf/spermwhale/types"

type DB interface {
	Get(key string, upperVersion uint64) (types.Value, error)
	Set(key string, val string, version uint64, writeIntent bool)
}
