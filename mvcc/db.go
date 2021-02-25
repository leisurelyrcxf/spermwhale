package mvcc

import "github.com/leisurelyrcxf/spermwhale/types"

type DB interface {
	types.KV
}
