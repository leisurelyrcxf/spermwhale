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

	ErrCodeKeyNotExists   = 27
	ErrCodeShardsNotReady = 28
	ErrCodeEmptyKey       = 29
	ErrCodeEmptyKeys      = 30

	ErrCodeVersionAlreadyExists = 50
	ErrCodeVersionNotExists     = 51

	ErrCodeTransactionAlreadyExists           = 62
	ErrCodeTransactionNotFound                = 63
	ErrCodeTransactionStateCorrupted          = 64
	ErrCodeTransactionInternalVersionOverflow = 65

	ErrCodeWriteIntentQueueFull = 70

	ErrCodeReadForWriteWaitFailed = 80
	ErrCodeReadForWriteQueueFull  = 81

	ErrCodeCantRemoveCommittedValue                  = 90
	ErrCodeTabletWriteTransactionNotFound            = 91
	ErrCodeTransactionRecordNotFoundAndWontBeWritten = 92
	ErrCodeSnapshotReadRetriedTooManyTimes           = 93

	ErrCodeNilResponse                                    = 99
	ErrCodeInvalidResponse                                = 100
	ErrCodeTxnRetriedTooManyTimes                         = 101
	ErrCodeNotSupported                                   = 111
	ErrCodeInvalidRequest                                 = 222
	ErrCodeSchedulerClosed                                = 333
	ErrCodeDontUseThisBeforeTaskFinished                  = 666
	ErrCodeGoRoutineExited                                = 667
	ErrCodeInvalidTopoData                                = 777
	ErrCodeCantGetOracle                                  = 888
	ErrCodeInvalidConfig                                  = 999
	ErrCodeReadForWriteTransactionCommitWithNoWrittenKeys = 1000
	ErrCodeReadForWriteReaderTimeouted                    = 1001
	ErrCodeUnknown                                        = 1111
	ErrCodeInject                                         = 11111
	ErrCodeDummy                                          = 12222
	ErrCodeAssertFailed                                   = 22222

	ErrCodePrevExists = 33333
)
