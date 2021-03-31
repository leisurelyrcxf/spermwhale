package errors

import (
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/consts"
)

var (
	ErrWriteReadConflict = &Error{
		Code: consts.ErrCodeWriteReadConflict,
		Msg:  "write read conflict",
	}
	ErrStaleWrite = &Error{
		Code: consts.ErrCodeStaleWrite,
		Msg:  "stale write",
	}
	ErrReadUncommittedDataPrevTxnStatusUndetermined = &Error{
		Code: consts.ErrCodeReadUncommittedDataPrevTxnStateUndetermined,
		Msg:  "read uncommitted data previous txn state undetermined",
	}
	ErrReadUncommittedDataPrevTxnKeyRollbacked = &Error{
		Code: consts.ErrCodeReadUncommittedDataPrevTxnKeyRollbacked,
		Msg:  "read uncommitted data previous txn key has been rollbacked",
	}
	ErrReadUncommittedDataPrevTxnToBeRollbacked = &Error{
		Code: consts.ErrCodeReadUncommittedDataPrevTxnToBeRollbacked,
		Msg:  "read uncommitted data previous txn to be rollbacked",
	}
	ErrReadAfterWriteFailed = &Error{
		Code: consts.ErrCodeReadAfterWriteFailed,
		Msg:  "read after write failed",
	}
	ErrTxnRollbacking = &Error{
		Code: consts.ErrCodeTxnRollbacking,
		Msg:  "txn rollbacking",
	}
	ErrTxnRollbacked = &Error{
		Code: consts.ErrCodeTxnRollbacked,
		Msg:  "txn rollbacked",
	}
	ErrShardsNotReady = &Error{
		Code: consts.ErrCodeShardsNotReady,
		Msg:  "shards not ready",
	}
	ErrKeyOrVersionNotExist = &Error{
		Code: consts.ErrCodeKeyOrVersionNotExists,
		Msg:  "key or version not exist",
	}
	ErrVersionAlreadyExists = &Error{
		Code: consts.ErrCodeVersionAlreadyExists,
		Msg:  "version already exists",
	}
	ErrNotSupported = &Error{
		Code: consts.ErrCodeNotSupported,
		Msg:  "not supported",
	}
	ErrNotAllowed = &Error{
		Code: consts.ErrCodeNotAllowed,
		Msg:  "not allowed",
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
	ErrTransactionInternalVersionOverflow = &Error{
		Code: consts.ErrCodeTransactionInternalVersionOverflow,
		Msg:  fmt.Sprintf("transaction internal version overflows %d", consts.MaxTxnInternalVersion),
	}
	ErrNilResponse = &Error{
		Code: consts.ErrCodeNilResponse,
		Msg:  "response is nil",
	}
	ErrInvalidResponse = &Error{
		Code: consts.ErrCodeInvalidResponse,
		Msg:  "response is invalid",
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
	ErrEmptyKey = &Error{
		Code: consts.ErrCodeEmptyKey,
		Msg:  "key is empty",
	}
	ErrEmptyKeys = &Error{
		Code: consts.ErrCodeEmptyKeys,
		Msg:  "keys are empty",
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
	ErrReadModifyWriteTransactionCommitWithNoWrittenKeys = &Error{
		Code: consts.ErrCodeReadModifyWriteTransactionCommitWithNoWrittenKeys,
		Msg:  "read modify write transaction commit with no written keys",
	}
	ErrReadModifyWriteWaitFailed = &Error{
		Code: consts.ErrCodeReadModifyWriteWaitFailed,
		Msg:  "read for write wait failed",
	}
	ErrReadModifyWriteQueueFull = &Error{
		Code: consts.ErrCodeReadModifyWriteQueueFull,
		Msg:  "read for write queue full, retry later",
	}
	ErrReadModifyWriteReaderTimeouted = &Error{
		Code: consts.ErrCodeReadModifyWriteReaderTimeouted,
		Msg:  "read for write reader timeouted",
	}
	ErrWriteIntentQueueFull = &Error{
		Code: consts.ErrCodeWriteIntentQueueFull,
		Msg:  "write intent queue full, retry later",
	}
	ErrTabletWriteTransactionNotFound = &Error{
		Code: consts.ErrCodeTabletWriteTransactionNotFound,
		Msg:  "tablet write transaction not found, probably removed",
	}
	ErrTransactionRecordNotFoundAndWontBeWritten = &Error{
		Code: consts.ErrCodeTransactionRecordNotFoundAndWontBeWritten,
		Msg:  "transaction record not found and prevented from being written", // help rollback if original txn coordinator was gone
	}
	ErrSnapshotReadRetriedTooManyTimes = &Error{
		Code: consts.ErrCodeSnapshotReadRetriedTooManyTimes,
		Msg:  "snapshot read retried too many times",
	}
	ErrReadVersionViolatesMinAllowedSnapshot = &Error{
		Code: consts.ErrCodeSnapshotReadVersionViolatesMinAllowedSnapshotVersion,
		Msg:  "reader version violates min allowed snapshot version",
	}
	ErrInvalidTxnSnapshotReadOption = &Error{
		Code: consts.ErrCodeInvalidTxnSnapshotReadOption,
		Msg:  "invalid TxnSnapshotReadOption",
	}
	ErrWriteKeyAfterTabletTxnRollbacked = &Error{
		Code: consts.ErrCodeWriteKeyAfterTabletTxnRollbacked,
		Msg:  "write key after tablet transaction rollbacked",
	}
)
