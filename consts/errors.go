package consts

import "github.com/leisurelyrcxf/spermwhale/utils/errors"

var (
	ErrTxnConflict    = errors.Errorf("transaction conflict")
	ErrShardNotExists = errors.Errorf("shard not exists")
)

const (
	ErrCodeOther = 1111
)
