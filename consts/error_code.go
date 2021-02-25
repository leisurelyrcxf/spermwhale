package consts

const (
	ErrCodeVersionConflict               = 1
	ErrCodeKeyNotExists                  = 2
	ErrCodeStaleWrite                    = 3
	ErrCodeShardsNotReady                = 5
	ErrCodeVersionNotExists              = 10
	ErrCodeTransactionAlreadyExists      = 11
	ErrCodeTransactionNotFound           = 12
	ErrCodeVersionNotExistsNeedsRollback = 15
	ErrCodeNotSupported                  = 111
	ErrCodeInvalidRequest                = 222
	ErrCodeUnknown                       = 1111
)
