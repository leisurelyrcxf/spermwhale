package consts

const (
	ErrCodeWriteReadConflict             = 1
	ErrCodeReadUncommittedData           = 2
	ErrCodeReadRollbackedData            = 3
	ErrCodeKeyNotExists                  = 6
	ErrCodeStaleWrite                    = 7
	ErrCodeShardsNotReady                = 8
	ErrCodeEmptyKey                      = 9
	ErrCodeVersionAlreadyExists          = 10
	ErrCodeVersionNotExists              = 11
	ErrCodeTransactionAlreadyExists      = 12
	ErrCodeTransactionNotFound           = 13
	ErrCodeTransactionStateCorrupted     = 14
	ErrCodeCantRemoveCommittedValue      = 15
	ErrCodeNilResponse                   = 55
	ErrCodeTxnRetriedTooManyTimes        = 100
	ErrCodeNotSupported                  = 111
	ErrCodeInvalidRequest                = 222
	ErrCodeSchedulerClosed               = 333
	ErrCodeReadAfterWriteFailed          = 555
	ErrCodeDontUseThisBeforeTaskFinished = 666
	ErrCodeInvalidTopoData               = 777
	ErrCodeCantGetOracle                 = 888
	ErrCodeInvalidConfig                 = 999
	ErrCodeUnknown                       = 1111
	ErrCodeInject                        = 11111
	ErrCodeDummy                         = 12222
	ErrCodeAssertFailed                  = 22222
)
