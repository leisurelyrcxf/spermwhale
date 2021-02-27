package errors

import (
	"github.com/leisurelyrcxf/spermwhale/consts"
)

var (
	ErrTransactionConflict = &Error{
		Code: consts.ErrCodeTransactionConflict,
		Msg:  "transaction conflict",
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
	ErrTransactionStateCorrupted = &Error{
		Code: consts.ErrCodeTransactionStateCorrupted,
		Msg:  "transaction state corrupted",
	}
	ErrNilResponse = &Error{
		Code: consts.ErrCodeNilResponse,
		Msg:  "response is nil",
	}
	ErrTxnRetriedTooManyTimes = &Error{
		Code: consts.ErrCodeTxnRetriedTooManyTimes,
		Msg:  "transaction retried too many times",
	}
	ErrInject = &Error{
		Code: consts.ErrCodeInject,
		Msg:  "injected error",
	}
	ErrAssertFailed = &Error{
		Code: consts.ErrCodeAssertFailed,
		Msg:  "assert failed",
	}
	ErrSchedulerClosed = &Error{
		Code: consts.ErrCodeSchedulerClosed,
		Msg:  "scheduler closed",
	}
	ErrFailedToWaitTask = &Error{
		Code: consts.ErrCodeFailedToWaitTask,
		Msg:  "failed to wait task",
	}
	ErrEmptyKey = &Error{
		Code: consts.ErrCodeEmptyKey,
		Msg:  "key is empty",
	}
)
