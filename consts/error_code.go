package consts

const (
	ErrCodeWriteReadConflict                           = 1
	ErrCodeStaleWrite                                  = 2
	ErrCodeReadUncommittedDataPrevTxnStateUndetermined = 3
	ErrCodeReadUncommittedDataPrevTxnKeyRollbacked     = 4
	ErrCodeReadUncommittedDataPrevTxnToBeRollbacked    = 5
	ErrCodeReadAfterWriteFailed                        = 6
	ErrCodeTxnRollbacking                              = 7
	ErrCodeTxnRollbacked                               = 8

	ErrCodeSnapshotReadRetriedTooManyTimes   = 19
	ErrCodeMinAllowedSnapshotVersionViolated = 20
	ErrCodeInvalidTxnSnapshotReadOption      = 21

	ErrCodeWriteKeyAfterTabletTxnRollbacked = 23

	ErrCodeKeyOrVersionNotExists = 27
	ErrCodeShardsNotReady        = 28
	ErrCodeEmptyKey              = 29
	ErrCodeEmptyKeys             = 30

	ErrCodeVersionAlreadyExists = 50

	ErrCodeTransactionAlreadyExists           = 62
	ErrCodeTransactionNotFound                = 63
	ErrCodeTransactionStateCorrupted          = 64
	ErrCodeTransactionInternalVersionOverflow = 65

	ErrCodeWriteIntentQueueFull         = 70
	ErrCodeTimestampCacheWriteQueueFull = 71

	ErrCodeReadModifyWriteWaitFailed = 80
	ErrCodeReadModifyWriteQueueFull  = 81

	ErrCodeCantRemoveCommittedValue                  = 90
	ErrCodeTabletWriteTransactionNotFound            = 91
	ErrCodeTransactionRecordNotFoundAndWontBeWritten = 92

	ErrCodeNilResponse                                       = 99
	ErrCodeInvalidResponse                                   = 100
	ErrCodeTxnRetriedTooManyTimes                            = 101
	ErrCodeNotSupported                                      = 111
	ErrCodeNotAllowed                                        = 222
	ErrCodeInvalidRequest                                    = 333
	ErrCodeSchedulerClosed                                   = 555
	ErrCodeDontUseThisBeforeTaskFinished                     = 666
	ErrCodeGoRoutineExited                                   = 667
	ErrCodeInvalidTopoData                                   = 777
	ErrCodeCantGetOracle                                     = 888
	ErrCodeInvalidConfig                                     = 999
	ErrCodeReadModifyWriteTransactionCommitWithNoWrittenKeys = 1000
	ErrCodeReadModifyWriteReaderTimeouted                    = 1001
	ErrCodeUnknown                                           = 1111
	ErrCodeInject                                            = 11111
	ErrCodeDummy                                             = 12222
	ErrCodeAssertFailed                                      = 22222

	ErrCodePrevExists = 33333
)
