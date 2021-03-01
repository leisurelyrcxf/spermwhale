package consts

const (
	ErrCodeTransactionConflict           = 1
	ErrCodeKeyNotExists                  = 2
	ErrCodeStaleWrite                    = 3
	ErrCodeShardsNotReady                = 5
	ErrCodeEmptyKey                      = 6
	ErrCodeVersionNotExists              = 10
	ErrCodeTransactionAlreadyExists      = 11
	ErrCodeTransactionNotFound           = 12
	ErrCodeTransactionStateCorrupted     = 13
	ErrCodeCantRemoveCommittedValue      = 14
	ErrCodeNilResponse                   = 55
	ErrCodeTxnRetriedTooManyTimes        = 100
	ErrCodeNotSupported                  = 111
	ErrCodeInvalidRequest                = 222
	ErrCodeSchedulerClosed               = 333
	ErrCodeReadFailedToWaitWriteTask     = 555
	ErrCodeDontUseThisBeforeTaskFinished = 666
	ErrCodeInvalidTopoData               = 777
	ErrCodeCantGetOracle                 = 888
	ErrCodeUnknown                       = 1111
	ErrCodeInject                        = 11111
	ErrCodeAssertFailed                  = 22222
)
