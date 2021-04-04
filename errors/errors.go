package errors

import (
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/consts"
)

type SubCodeGenerator int32

func NewSubCodeGenerator() *SubCodeGenerator {
	g := new(SubCodeGenerator)
	*g = -1
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
		Code: consts.ErrCodeReadUncommittedDataPrevTxnStateUndetermined,
		Msg:  "read uncommitted data previous txn state undetermined",
	})
	ErrReadUncommittedDataPrevTxnKeyRollbacked = registerErr(&Error{
		Code: consts.ErrCodeReadUncommittedDataPrevTxnKeyRollbacked,
		Msg:  "read uncommitted data previous txn key has been rollbacked",
	})
	ErrReadUncommittedDataPrevTxnKeyRollbackedReadAfterWrite = registerErr(&Error{
		Code:    consts.ErrCodeReadUncommittedDataPrevTxnKeyRollbacked,
		SubCode: 1,
		Msg:     "read uncommitted data previous txn key has been rollbacked: read after write",
	})
	ErrReadUncommittedDataPrevTxnToBeRollbacked = registerErr(&Error{
		Code: consts.ErrCodeReadUncommittedDataPrevTxnToBeRollbacked,
		Msg:  "read uncommitted data previous txn to be rollbacked",
	})
	ErrReadAfterWriteFailed = registerErr(&Error{
		Code: consts.ErrCodeReadAfterWriteFailed,
		Msg:  "read after write failed",
	})
	ErrTxnRollbacking = registerErr(&Error{
		Code: consts.ErrCodeTxnRollbacking,
		Msg:  "txn rollbacking",
	})
	ErrTxnRollbacked = registerErr(&Error{
		Code: consts.ErrCodeTxnRollbacked,
		Msg:  "txn rollbacked",
	})
	ErrShardsNotReady = registerErr(&Error{
		Code: consts.ErrCodeShardsNotReady,
		Msg:  "shards not ready",
	})
	ErrKeyOrVersionNotExist = registerErr(&Error{
		Code: consts.ErrCodeKeyOrVersionNotExists,
		Msg:  "key or version not exist",
	})
	ErrVersionAlreadyExists = registerErr(&Error{
		Code: consts.ErrCodeVersionAlreadyExists,
		Msg:  "version already exists",
	})
	ErrNotSupported = registerErr(&Error{
		Code: consts.ErrCodeNotSupported,
		Msg:  "not supported",
	})
	ErrNotAllowed = registerErr(&Error{
		Code: consts.ErrCodeNotAllowed,
		Msg:  "not allowed",
	})
	ErrInvalidRequest = registerErr(&Error{
		Code: consts.ErrCodeInvalidRequest,
		Msg:  "invalid request",
	})
	ErrTxnExists = registerErr(&Error{
		Code: consts.ErrCodeTransactionAlreadyExists,
		Msg:  "transaction already exists",
	})
	ErrTransactionNotFound = registerErr(&Error{
		Code: consts.ErrCodeTransactionNotFound,
		Msg:  "transaction not found",
	})
	ErrTransactionStateCorrupted = registerErr(&Error{
		Code: consts.ErrCodeTransactionStateCorrupted,
		Msg:  "transaction state corrupted",
	})
	ErrTransactionInternalVersionOverflow = registerErr(&Error{
		Code: consts.ErrCodeTransactionInternalVersionOverflow,
		Msg:  fmt.Sprintf("transaction internal version overflows %d", consts.MaxTxnInternalVersion),
	})
	ErrNilResponse = registerErr(&Error{
		Code: consts.ErrCodeNilResponse,
		Msg:  "response is nil",
	})
	ErrInvalidResponse = registerErr(&Error{
		Code: consts.ErrCodeInvalidResponse,
		Msg:  "response is invalid",
	})
	ErrTxnRetriedTooManyTimes = registerErr(&Error{
		Code: consts.ErrCodeTxnRetriedTooManyTimes,
		Msg:  "transaction retried too many times",
	})
	ErrInject = registerErr(&Error{
		Code: consts.ErrCodeInject,
		Msg:  "injected error",
	})
	ErrDummy = registerErr(&Error{
		Code: consts.ErrCodeDummy,
		Msg:  "dummy error",
	})
	ErrAssertFailed = registerErr(&Error{
		Code: consts.ErrCodeAssertFailed,
		Msg:  "assert failed",
	})
	ErrSchedulerClosed = registerErr(&Error{
		Code: consts.ErrCodeSchedulerClosed,
		Msg:  "scheduler closed",
	})
	ErrEmptyKey = registerErr(&Error{
		Code: consts.ErrCodeEmptyKey,
		Msg:  "key is empty",
	})
	ErrEmptyKeys = registerErr(&Error{
		Code: consts.ErrCodeEmptyKeys,
		Msg:  "keys are empty",
	})
	ErrDontUseThisBeforeTaskFinished = registerErr(&Error{
		Code: consts.ErrCodeDontUseThisBeforeTaskFinished,
		Msg:  "don't use this before task finish",
	})
	ErrGoRoutineExited = registerErr(&Error{
		Code: consts.ErrCodeGoRoutineExited,
		Msg:  "go routine exited",
	})
	ErrCantRemoveCommittedValue = registerErr(&Error{
		Code: consts.ErrCodeCantRemoveCommittedValue,
		Msg:  "can't remove committed value",
	})
	ErrInvalidTopoData = registerErr(&Error{
		Code: consts.ErrCodeInvalidTopoData,
		Msg:  "invalid topo data",
	})
	ErrCantGetOracle = registerErr(&Error{
		Code: consts.ErrCodeCantGetOracle,
		Msg:  "can't get oracle",
	})
	ErrInvalidConfig = registerErr(&Error{
		Code: consts.ErrCodeInvalidConfig,
		Msg:  "invalid config",
	})
	ErrReadModifyWriteTransactionCommitWithNoWrittenKeys = registerErr(&Error{
		Code: consts.ErrCodeReadModifyWriteTransactionCommitWithNoWrittenKeys,
		Msg:  "read modify write transaction commit with no written keys",
	})
	ErrReadModifyWriteWaitFailed = registerErr(&Error{
		Code: consts.ErrCodeReadModifyWriteWaitFailed,
		Msg:  "read for write wait failed",
	})
	ErrReadModifyWriteQueueFull = registerErr(&Error{
		Code: consts.ErrCodeReadModifyWriteQueueFull,
		Msg:  "read for write queue full, retry later",
	})
	ErrReadModifyWriteReaderTimeouted = registerErr(&Error{
		Code: consts.ErrCodeReadModifyWriteReaderTimeouted,
		Msg:  "read for write reader timeouted",
	})
	ErrTimestampCacheWriteQueueFull = registerErr(&Error{
		Code: consts.ErrCodeTimestampCacheWriteQueueFull,
		Msg:  "timestamp cache write queue full, retry later",
	})
	ErrWriteIntentQueueFull = registerErr(&Error{
		Code: consts.ErrCodeWriteIntentQueueFull,
		Msg:  "write intent queue full, retry later",
	})
	ErrWaitKeyEventFailed = registerErr(&Error{
		Code: consts.ErrCodeWaitKeyEventFailed,
		Msg:  "wait key event failed",
	})
	ErrTabletWriteTransactionNotFound = registerErr(&Error{
		Code: consts.ErrCodeTabletWriteTransactionNotFound,
		Msg:  "tablet write transaction not found, probably removed",
	})
	ErrTransactionRecordNotFoundAndWontBeWritten = registerErr(&Error{
		Code: consts.ErrCodeTransactionRecordNotFoundAndWontBeWritten,
		Msg:  "transaction record not found and prevented from being written", // help rollback if original txn coordinator was gone
	})
	ErrTransactionRecordNotFoundAndFoundAbortedValue = registerErr(&Error{
		Code: consts.ErrCodeTransactionRecordNotFoundAndFoundRollbackedValue,
		Msg:  "transaction record not found and found aborted value", // help rollback if original txn coordinator was gone
	})
	ErrPrevWriterNotFinishedYet = registerErr(&Error{
		Code: consts.ErrCodePrevWriterNotFinishedYet,
		Msg:  "prev writer not finished yet",
	})
	ErrInternalVersionSmallerThanPrevWriter = registerErr(&Error{
		Code: consts.ErrCodeInternalVersionSmallerThanPrevWriter,
		Msg:  "internal version smaller than previous writer",
	})
	ErrSnapshotReadRetriedTooManyTimes = registerErr(&Error{
		Code: consts.ErrCodeSnapshotReadRetriedTooManyTimes,
		Msg:  "snapshot read retried too many times",
	})
	ErrMinAllowedSnapshotVersionViolated = registerErr(&Error{
		Code: consts.ErrCodeMinAllowedSnapshotVersionViolated,
		Msg:  "min allowed snapshot version violated",
	})
	ErrInvalidTxnSnapshotReadOption = registerErr(&Error{
		Code: consts.ErrCodeInvalidTxnSnapshotReadOption,
		Msg:  "invalid TxnSnapshotReadOption",
	})
	ErrWriteKeyAfterTabletTxnRollbacked = registerErr(&Error{
		Code: consts.ErrCodeWriteKeyAfterTabletTxnRollbacked,
		Msg:  "write key after tablet transaction rollbacked",
	})
	ErrRemoveKeyFailed = registerErr(&Error{
		Code: consts.ErrCodeRemoveKeyFailed,
		Msg:  "remove key failed",
	})
)
