package errors

import (
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/consts"
)

type SubCodeGenerator int32

func NewSubCodeGenerator() *SubCodeGenerator {
	g := new(SubCodeGenerator)
	*g = consts.ErrSubCodeUnknown
	return g
}

func (g *SubCodeGenerator) Next() int32 {
	*g++
	return int32(*g)
}

var (
	ErrWriteReadConflictSubCodeGenerator = NewSubCodeGenerator()
	ErrWriteReadConflict                 = registerErr(&Error{
		Code:    consts.ErrCodeWriteReadConflict,
		SubCode: ErrWriteReadConflictSubCodeGenerator.Next(),
		Msg:     "write read conflict",
	})
	ErrWriteReadConflictReaderSkippedCommittedData = registerErr(&Error{
		Code:    consts.ErrCodeWriteReadConflict,
		SubCode: ErrWriteReadConflictSubCodeGenerator.Next(),
		Msg:     "write read conflict: reader skipped committed data",
	})
	ErrWriteReadConflictUnsafeRead = registerErr(&Error{
		Code:    consts.ErrCodeWriteReadConflict,
		SubCode: ErrWriteReadConflictSubCodeGenerator.Next(),
		Msg:     "write read conflict: unsafe read",
	})
	ErrWriteReadConflictUnsafeReadWaitTxnTerminateFailed = registerErr(&Error{
		Code:    consts.ErrCodeWriteReadConflict,
		SubCode: ErrWriteReadConflictSubCodeGenerator.Next(),
		Msg:     "write read conflict: unsafe read wait txn terminate failed",
	})
	ErrWriteReadConflictFutureWritePrevented = registerErr(&Error{
		Code:    consts.ErrCodeWriteReadConflict,
		SubCode: ErrWriteReadConflictSubCodeGenerator.Next(),
		Msg:     "write read conflict: future write prevented",
	})

	ErrStaleWriteSubCodeGenerator = NewSubCodeGenerator()
	ErrStaleWrite                 = registerErr(&Error{
		Code:    consts.ErrCodeStaleWrite,
		SubCode: ErrStaleWriteSubCodeGenerator.Next(),
		Msg:     "stale write",
	})
	ErrStaleWriteWriterVersionSmallerThanMaxRemovedWriterVersion = registerErr(&Error{
		Code:    consts.ErrCodeStaleWrite,
		SubCode: ErrStaleWriteSubCodeGenerator.Next(),
		Msg:     "stale write: writer version smaller than max removed",
	})
	ErrStaleWriteInsertTooOldTxn = registerErr(&Error{
		Code:    consts.ErrCodeStaleWrite,
		SubCode: ErrStaleWriteSubCodeGenerator.Next(),
		Msg:     "stale write: insert too old txn",
	})

	ErrReadUncommittedDataPrevTxnStatusUndetermined = registerErr(&Error{
		Code:    consts.ErrCodeReadUncommittedDataPrevTxnStateUndetermined,
		SubCode: 1,
		Msg:     "read uncommitted data previous txn state undetermined",
	})
	ErrReadUncommittedDataPrevTxnKeyRollbacked = registerErr(&Error{
		Code:    consts.ErrCodeReadUncommittedDataPrevTxnKeyRollbacked,
		SubCode: 1,
		Msg:     "read uncommitted data previous txn key has been rollbacked",
	})
	ErrReadUncommittedDataPrevTxnKeyRollbackedReadAfterWrite = registerErr(&Error{
		Code:    consts.ErrCodeReadUncommittedDataPrevTxnKeyRollbacked,
		SubCode: 2,
		Msg:     "read uncommitted data previous txn key has been rollbacked: read after write",
	})
	ErrReadUncommittedDataPrevTxnToBeRollbacked = registerErr(&Error{
		Code:    consts.ErrCodeReadUncommittedDataPrevTxnToBeRollbacked,
		SubCode: 1,
		Msg:     "read uncommitted data previous txn to be rollbacked",
	})
	ErrReadAfterWriteFailed = registerErr(&Error{
		Code:    consts.ErrCodeReadAfterWriteFailed,
		SubCode: 1,
		Msg:     "read after write failed",
	})
	ErrTxnRollbacking = registerErr(&Error{
		Code:    consts.ErrCodeTxnRollbacking,
		SubCode: 1,
		Msg:     "txn rollbacking",
	})
	ErrTxnRollbacked = registerErr(&Error{
		Code:    consts.ErrCodeTxnRollbacked,
		SubCode: 1,
		Msg:     "txn rollbacked",
	})
	ErrShardsNotReady = registerErr(&Error{
		Code:    consts.ErrCodeShardsNotReady,
		SubCode: 1,
		Msg:     "shards not ready",
	})
	ErrKeyOrVersionNotExist = registerErr(&Error{
		Code:    consts.ErrCodeKeyOrVersionNotExists,
		SubCode: consts.ErrKeyOrVersionNotExistsSubCodeNotExistsInDB,
		Msg:     "key or version not exist",
	})
	ErrKeyOrVersionNotExistExistsButToBeRollbacked = registerErr(&Error{
		Code:    consts.ErrCodeKeyOrVersionNotExists,
		SubCode: consts.ErrKeyOrVersionNotExistsSubCodeExistsButToBeRollbacked,
		Msg:     "key or version not exist: exists but to be rollbacked",
	})
	ErrVersionAlreadyExists = registerErr(&Error{
		Code:    consts.ErrCodeVersionAlreadyExists,
		SubCode: 1,
		Msg:     "version already exists",
	})
	ErrNotSupported = registerErr(&Error{
		Code:    consts.ErrCodeNotSupported,
		SubCode: 1,
		Msg:     "not supported",
	})
	ErrNotAllowed = registerErr(&Error{
		Code:    consts.ErrCodeNotAllowed,
		SubCode: 1,
		Msg:     "not allowed",
	})
	ErrInvalidRequest = registerErr(&Error{
		Code:    consts.ErrCodeInvalidRequest,
		SubCode: 1,
		Msg:     "invalid request",
	})
	ErrTxnExists = registerErr(&Error{
		Code:    consts.ErrCodeTransactionAlreadyExists,
		SubCode: 1,
		Msg:     "transaction already exists",
	})
	ErrTransactionNotFound = registerErr(&Error{
		Code:    consts.ErrCodeTransactionNotFound,
		SubCode: 1,
		Msg:     "transaction not found",
	})
	ErrTransactionStateCorrupted = registerErr(&Error{
		Code:    consts.ErrCodeTransactionStateCorrupted,
		SubCode: 1,
		Msg:     "transaction state corrupted",
	})
	ErrTransactionInternalVersionOverflow = registerErr(&Error{
		Code:    consts.ErrCodeTransactionInternalVersionOverflow,
		SubCode: 1,
		Msg:     fmt.Sprintf("transaction internal version overflows %d", consts.MaxTxnInternalVersion),
	})
	ErrNilResponse = registerErr(&Error{
		Code:    consts.ErrCodeNilResponse,
		SubCode: 1,
		Msg:     "response is nil",
	})
	ErrInvalidResponse = registerErr(&Error{
		Code:    consts.ErrCodeInvalidResponse,
		SubCode: 1,
		Msg:     "response is invalid",
	})
	ErrTxnRetriedTooManyTimes = registerErr(&Error{
		Code:    consts.ErrCodeTxnRetriedTooManyTimes,
		SubCode: 1,
		Msg:     "transaction retried too many times",
	})
	ErrInject = registerErr(&Error{
		Code:    consts.ErrCodeInject,
		SubCode: 1,
		Msg:     "injected error",
	})
	ErrInjectAckLost = registerErr(&Error{
		Code:    consts.ErrCodeInject,
		SubCode: 2,
		Msg:     "injected error: ack lost",
	})
	ErrDummy = registerErr(&Error{
		Code:    consts.ErrCodeDummy,
		SubCode: 1,
		Msg:     "dummy error",
	})
	ErrAssertFailed = registerErr(&Error{
		Code:    consts.ErrCodeAssertFailed,
		SubCode: 1,
		Msg:     "assert failed",
	})
	ErrSchedulerClosed = registerErr(&Error{
		Code:    consts.ErrCodeSchedulerClosed,
		SubCode: 1,
		Msg:     "scheduler closed",
	})
	ErrEmptyKey = registerErr(&Error{
		Code:    consts.ErrCodeEmptyKey,
		SubCode: 1,
		Msg:     "key is empty",
	})
	ErrEmptyKeys = registerErr(&Error{
		Code:    consts.ErrCodeEmptyKeys,
		SubCode: 1,
		Msg:     "keys are empty",
	})
	ErrDontUseThisBeforeTaskFinished = registerErr(&Error{
		Code:    consts.ErrCodeDontUseThisBeforeTaskFinished,
		SubCode: 1,
		Msg:     "don't use this before task finish",
	})
	ErrGoRoutineExited = registerErr(&Error{
		Code:    consts.ErrCodeGoRoutineExited,
		SubCode: 1,
		Msg:     "go routine exited",
	})
	ErrCantRemoveCommittedValue = registerErr(&Error{
		Code:    consts.ErrCodeCantRemoveCommittedValue,
		SubCode: 1,
		Msg:     "can't remove committed value",
	})
	ErrInvalidTopoData = registerErr(&Error{
		Code:    consts.ErrCodeInvalidTopoData,
		SubCode: 1,
		Msg:     "invalid topo data",
	})
	ErrCantGetOracle = registerErr(&Error{
		Code:    consts.ErrCodeCantGetOracle,
		SubCode: 1,
		Msg:     "can't get oracle",
	})
	ErrInvalidConfig = registerErr(&Error{
		Code:    consts.ErrCodeInvalidConfig,
		SubCode: 1,
		Msg:     "invalid config",
	})
	ErrReadModifyWriteTransactionCommitWithNoWrittenKeys = registerErr(&Error{
		Code:    consts.ErrCodeReadModifyWriteTransactionCommitWithNoWrittenKeys,
		SubCode: 1,
		Msg:     "read modify write transaction commit with no written keys",
	})
	ErrReadModifyWriteWaitFailed = registerErr(&Error{
		Code:    consts.ErrCodeReadModifyWriteWaitFailed,
		SubCode: 1,
		Msg:     "read for write wait failed",
	})
	ErrReadModifyWriteQueueFull = registerErr(&Error{
		Code:    consts.ErrCodeReadModifyWriteQueueFull,
		SubCode: 1,
		Msg:     "read for write queue full, retry later",
	})
	ErrReadModifyWriteReaderTimeouted = registerErr(&Error{
		Code:    consts.ErrCodeReadModifyWriteReaderTimeouted,
		SubCode: 1,
		Msg:     "read for write reader timeouted",
	})
	ErrTimestampCacheWriteQueueFull = registerErr(&Error{
		Code:    consts.ErrCodeTimestampCacheWriteQueueFull,
		SubCode: 1,
		Msg:     "timestamp cache write queue full, retry later",
	})
	ErrWriteIntentQueueFull = registerErr(&Error{
		Code:    consts.ErrCodeWriteIntentQueueFull,
		SubCode: 1,
		Msg:     "write intent queue full, retry later",
	})
	ErrWaitKeyEventFailed = registerErr(&Error{
		Code:    consts.ErrCodeWaitKeyEventFailed,
		SubCode: 1,
		Msg:     "wait key event failed",
	})
	ErrTabletWriteTransactionNotFound = registerErr(&Error{
		Code:    consts.ErrCodeTabletWriteTransactionNotFound,
		SubCode: 1,
		Msg:     "tablet write transaction not found, probably removed",
	})
	ErrTransactionRecordNotFoundAndWontBeWritten = registerErr(&Error{
		Code:    consts.ErrCodeTransactionRecordNotFoundAndWontBeWritten,
		SubCode: 1,
		Msg:     "transaction record not found and prevented from being written", // help rollback if original txn coordinator was gone
	})
	ErrTransactionRecordNotFoundAndFoundAbortedValue = registerErr(&Error{
		Code:    consts.ErrCodeTransactionRecordNotFoundAndFoundRollbackedValue,
		SubCode: 1,
		Msg:     "transaction record not found and found aborted value", // help rollback if original txn coordinator was gone
	})
	ErrTransactionRecordAborted = registerErr(&Error{
		Code:    consts.ErrCodeTransactionRecordAborted,
		SubCode: 1,
		Msg:     "transaction record aborted", // help rollback if original txn coordinator was gone
	})
	ErrPrevWriterNotFinishedYet = registerErr(&Error{
		Code:    consts.ErrCodePrevWriterNotFinishedYet,
		SubCode: 1,
		Msg:     "prev writer not finished yet",
	})
	ErrInternalVersionSmallerThanPrevWriter = registerErr(&Error{
		Code:    consts.ErrCodeInternalVersionSmallerThanPrevWriter,
		SubCode: 1,
		Msg:     "internal version smaller than previous writer",
	})
	ErrSnapshotReadRetriedTooManyTimes = registerErr(&Error{
		Code:    consts.ErrCodeSnapshotReadRetriedTooManyTimes,
		SubCode: 1,
		Msg:     "snapshot read retried too many times",
	})
	ErrMinAllowedSnapshotVersionViolated = registerErr(&Error{
		Code:    consts.ErrCodeMinAllowedSnapshotVersionViolated,
		SubCode: 1,
		Msg:     "min allowed snapshot version violated",
	})
	ErrInvalidTxnSnapshotReadOption = registerErr(&Error{
		Code:    consts.ErrCodeInvalidTxnSnapshotReadOption,
		SubCode: 1,
		Msg:     "invalid TxnSnapshotReadOption",
	})
	ErrWriteKeyAfterTabletTxnRollbacked = registerErr(&Error{
		Code:    consts.ErrCodeWriteKeyAfterTabletTxnRollbacked,
		SubCode: 1,
		Msg:     "write key after tablet transaction rollbacked",
	})
	ErrTabletTxnSetFailedKeyNotFound = registerErr(&Error{
		Code:    consts.ErrCodeTabletTxnSetFailedKeyNotFound,
		SubCode: 1,
		Msg:     "tablet txn set failed and key not found",
	})
	ErrTabletTxnSetFailedKeyStatusUndetermined = registerErr(&Error{
		Code:    consts.ErrCodeTabletTxnSetFailedKeyStatusUndetermined,
		SubCode: 1,
		Msg:     "tablet txn set failed and can't determine key status",
	})
	ErrRemoveKeyFailed = registerErr(&Error{
		Code:    consts.ErrCodeRemoveKeyFailed,
		SubCode: 1,
		Msg:     "remove key failed",
	})
	UnreachableCode = registerErr(&Error{
		Code:    consts.ErrCodeUnreachableCode,
		SubCode: 1,
		Msg:     "unreachable code",
	})
)
