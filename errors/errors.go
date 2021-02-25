package errors

import (
	"github.com/leisurelyrcxf/spermwhale/consts"
)

var (
	ErrTxnConflict     = Errorf("transaction conflict")
	ErrShardNotExists  = Errorf("shard not exists")
	ErrVersionConflict = &Error{
		Code: consts.ErrCodeVersionConflict,
		Msg:  "version conflict",
	}
	ErrStaleWrite = &Error{
		Code: consts.ErrCodeStaleWrite,
		Msg:  "stale write",
	}
	ErrShardsNotReady = &Error{
		Code: consts.ErrCodeShardsNotReady,
		Msg:  "shards not ready",
	}
	ErrKeyNotExist = &Error{
		Code: consts.ErrCodeKeyNotExists,
		Msg:  "key not exist",
	}
	ErrVersionNotExists = &Error{
		Code: consts.ErrCodeVersionNotExists,
		Msg:  "version not exist",
	}
	ErrNotSupported = &Error{
		Code: consts.ErrCodeNotSupported,
		Msg:  "not supported",
	}
	ErrInvalidRequest = &Error{
		Code: consts.ErrCodeInvalidRequest,
		Msg:  "invalid request",
	}
	ErrVersionNotExistsNeedsRollback = &Error{
		Code: consts.ErrCodeVersionNotExistsNeedsRollback,
		Msg:  "key or version not exists needs rollback",
	}
	ErrTxnExists = &Error{
		Code: consts.ErrCodeTransactionAlreadyExists,
		Msg:  "transaction already exists",
	}
	ErrTransactionNotFound = &Error{
		Code: consts.ErrCodeTransactionNotFound,
		Msg:  "transaction not found",
	}
)
