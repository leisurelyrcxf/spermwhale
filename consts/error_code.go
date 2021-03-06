package consts

const (
	ErrCodeUnknown    int32 = 0
	ErrSubCodeUnknown       = 0

	ErrCodeWriteReadConflict                           int32 = 1
	ErrCodeStaleWrite                                        = 2
	ErrCodeReadUncommittedDataPrevTxnStateUndetermined       = 3

	ErrCodeReadUncommittedDataPrevTxnAborted                        = 4
	ErrSubCodeReadUncommittedDataPrevTxnRollbacking                 = 1
	ErrSubCodeReadUncommittedDataPrevTxnRollbacked                  = 2
	ErrSubCodeReadUncommittedDataPrevTxnKeyRollbackedReadAfterWrite = 3

	ErrCodeReadAfterWriteFailed = 5
	ErrCodeTxnRollbacking       = 6
	ErrCodeTxnRollbacked        = 7

	ErrCodeSnapshotReadRetriedTooManyTimes   int32 = 19
	ErrCodeMinAllowedSnapshotVersionViolated       = 20
	ErrCodeInvalidTxnSnapshotReadOption            = 21

	ErrCodeWriteKeyAfterTabletTxnRollbacked        int32 = 23
	ErrCodeTabletTxnSetFailedKeyNotFound           int32 = 24
	ErrCodeTabletTxnSetFailedKeyStatusUndetermined int32 = 25

	ErrCodeKeyOrVersionNotExists                            int32 = 27
	ErrSubCodeKeyOrVersionNotExistsExistsInDBButRollbacking       = 1
	ErrSubCodeKeyOrVersionNotExistsInDB                           = 2

	ErrCodeShardsNotReady = 28
	ErrCodeEmptyKey       = 29
	ErrCodeEmptyKeys      = 30

	ErrCodeVersionAlreadyExists int32 = 50

	ErrCodeTransactionAlreadyExists           int32 = 62
	ErrCodeTransactionNotFound                      = 63
	ErrCodeTransactionStateCorrupted          int32 = 64
	ErrCodeTransactionInternalVersionOverflow       = 65

	ErrCodeWriteIntentQueueFull         int32 = 70
	ErrCodeWaitKeyEventFailed                 = 71
	ErrCodeTimestampCacheWriteQueueFull       = 72

	ErrCodeReadModifyWriteWaitFailed int32 = 80
	ErrCodeReadModifyWriteQueueFull        = 81

	ErrCodeCantRemoveCommittedValue                         int32 = 90
	ErrCodeTabletWriteTransactionNotFound                         = 91
	ErrCodeTransactionRecordNotFoundAndWontBeWritten              = 92
	ErrCodeTransactionRecordNotFoundAndFoundRollbackedValue       = 93
	ErrCodeTransactionRecordAborted                               = 94
	ErrCodePrevWriterNotFinishedYet                               = 95
	ErrCodeInternalVersionSmallerThanPrevWriter                   = 96

	ErrCodeNilResponse                                       int32 = 99
	ErrCodeInvalidResponse                                         = 100
	ErrCodeTxnRetriedTooManyTimes                                  = 101
	ErrCodeRemoveKeyFailed                                         = 102
	ErrCodeNotSupported                                            = 111
	ErrCodeNotAllowed                                              = 222
	ErrCodeInvalidRequest                                          = 333
	ErrCodeSchedulerClosed                                         = 555
	ErrCodeDontUseThisBeforeTaskFinished                           = 666
	ErrCodeGoRoutineExited                                         = 667
	ErrCodeInvalidTopoData                                         = 777
	ErrCodeCantGetOracle                                           = 888
	ErrCodeInvalidConfig                                           = 999
	ErrCodeReadModifyWriteTransactionCommitWithNoWrittenKeys       = 1000
	ErrCodeReadModifyWriteReaderTimeouted                          = 1001
	ErrCodePrevExists                                              = 1002
	ErrCodeInject                                                  = 11111
	ErrCodeDummy                                                   = 12222
	ErrCodeAssertFailed                                            = 22222
	ErrCodeUnreachableCode                                         = 33333
)
