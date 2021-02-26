package consts

const (
	ErrCodeTransactionConflict           = 1
	ErrCodeKeyNotExists                  = 2
	ErrCodeStaleWrite                    = 3
	ErrCodeShardsNotReady                = 5
	ErrCodeVersionNotExists              = 10
	ErrCodeTransactionAlreadyExists      = 11
	ErrCodeTransactionNotFound           = 12
	ErrCodeTransactionStateCorrupted     = 13
	ErrCodeVersionNotExistsNeedsRollback = 15
	ErrCodeNilResponse                   = 55
	ErrCodeTxnRetriedTooManyTimes        = 100
	ErrCodeNotSupported                  = 111
	ErrCodeInvalidRequest                = 222
	ErrCodeCancelledDueToParentFailed    = 333
	ErrCodeSchedulerClosed               = 444
	ErrCodeErrFailedToWaitTask           = 555
	ErrCodeUnknown                       = 1111
	ErrCodeInject                        = 11111
	ErrCodeAssertFailed                  = 22222
)
