package errors

import (
	"github.com/leisurelyrcxf/spermwhale/consts"
)

var (
	ErrWriteReadConflict = &Error{
		Code: consts.ErrCodeWriteReadConflict,
		Msg:  "write read conflict",
	}
	ErrReadUncommittedDataPrevTxnStatusUndetermined = &Error{
		Code: consts.ErrCodeReadUncommittedDataPrevTxnStateUndetermined,
		Msg:  "read uncommitted data previous txn state undetermined",
	}
	ErrReadUncommittedDataPrevTxnHasBeenRollbacked = &Error{
		Code: consts.ErrCodeReadUncommittedDataPrevTxnHasBeenRollbacked,
		Msg:  "read uncommitted data previous txn has been rollbacked",
	}
	ErrReadUncommittedDataPrevTxnToBeRollbacked = &Error{
		Code: consts.ErrCodeReadUncommittedDataPrevTxnToBeRollbacked,
		Msg:  "read uncommitted data previous txn to be rollbacked",
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
	ErrVersionAlreadyExists = &Error{
		Code: consts.ErrCodeVersionAlreadyExists,
		Msg:  "version already exists",
	}
	ErrNotSupported = &Error{
		Code: consts.ErrCodeNotSupported,
		Msg:  "not supported",
	}
	ErrInvalidRequest = &Error{
		Code: consts.ErrCodeInvalidRequest,
		Msg:  "invalid request",
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
	ErrDummy = &Error{
		Code: consts.ErrCodeDummy,
		Msg:  "dummy error",
	}
	ErrAssertFailed = &Error{
		Code: consts.ErrCodeAssertFailed,
		Msg:  "assert failed",
	}
	ErrSchedulerClosed = &Error{
		Code: consts.ErrCodeSchedulerClosed,
		Msg:  "scheduler closed",
	}
	ErrReadAfterWriteFailed = &Error{
		Code: consts.ErrCodeReadAfterWriteFailed,
		Msg:  "read after write failed",
	}
	ErrEmptyKey = &Error{
		Code: consts.ErrCodeEmptyKey,
		Msg:  "key is empty",
	}
	ErrDontUseThisBeforeTaskFinished = &Error{
		Code: consts.ErrCodeDontUseThisBeforeTaskFinished,
		Msg:  "don't use this before task finish",
	}
	ErrGoRoutineExited = &Error{
		Code: consts.ErrCodeGoRoutineExited,
		Msg:  "go routine exited",
	}
	ErrCantRemoveCommittedValue = &Error{
		Code: consts.ErrCodeCantRemoveCommittedValue,
		Msg:  "can't remove committed value",
	}
	ErrInvalidTopoData = &Error{
		Code: consts.ErrCodeInvalidTopoData,
		Msg:  "invalid topo data",
	}
	ErrCantGetOracle = &Error{
		Code: consts.ErrCodeCantGetOracle,
		Msg:  "can't get oracle",
	}
	ErrInvalidConfig = &Error{
		Code: consts.ErrCodeInvalidConfig,
		Msg:  "invalid config",
	}
	ErrReadForWriteTransactionCommitWithNoWrittenKeys = &Error{
		Code: consts.ErrCodeReadForWriteTransactionCommitWithNoWrittenKeys,
		Msg:  "read for write transaction commit with no written keys",
	}
	ErrReadForWriteWaitFailed = &Error{
		Code: consts.ErrCodeReadForWriteWaitFailed,
		Msg:  "read for write wait failed",
	}
	ErrReadForWriteQueueFull = &Error{
		Code: consts.ErrCodeReadForWriteQueueFull,
		Msg:  "read for write queue full, retry later",
	}
	ErrWriteIntentQueueFull = &Error{
		Code: consts.ErrCodeWriteIntentQueueFull,
		Msg:  "write intent queue full, retry later",
	}
	ErrTabletWriteTransactionNotFound = &Error{
		Code: consts.ErrCodeTabletWriteTransactionNotFound,
		Msg:  "tablet write transaction not found, probably removed",
	}
)
