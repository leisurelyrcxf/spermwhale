package consts

import (
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils/errors"
)

const (
	ErrCodeVersionConflict = 1
	ErrCodeStaleWrite      = 3
	ErrCodeShardsNotReady  = 5
	ErrCodeUnknown         = 1111
)

var (
	ErrTxnConflict        = errors.Errorf("transaction conflict")
	ErrShardNotExists     = errors.Errorf("shard not exists")
	ErrMsgVersionConflict = "version conflict"
	ErrStaleWrite         = &types.Error{
		Code: ErrCodeStaleWrite,
		Msg:  "stale write",
	}
	ErrShardsNotReady = &types.Error{
		Code: ErrCodeShardsNotReady,
		Msg:  "shards not ready",
	}
)
