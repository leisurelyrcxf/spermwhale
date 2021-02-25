package errors

import (
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/consts"
)

type Error struct {
	Code int
	Msg  string
}

func NewError(code int, msg string) *Error {
	return &Error{
		Code: code,
		Msg:  msg,
	}
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%v, err_code:%v", e.Msg, e.Code)
}

func IsNotExistsErr(e error) bool {
	ve, ok := e.(*Error)
	if !ok {
		return false
	}
	return ve.Code == consts.ErrCodeKeyNotExists || ve.Code == consts.ErrCodeVersionNotExists ||
		ve.Code == consts.ErrCodeVersionNotExistsNeedsRollback
}

func IsNeedsRollbackErr(e error) bool {
	ve, ok := e.(*Error)
	if !ok {
		return false
	}
	return ve.Code == consts.ErrCodeVersionNotExistsNeedsRollback
}

func IsRetryableErr(e error) bool {
	ve, ok := e.(*Error)
	if !ok {
		return false
	}
	return ve.Code == consts.ErrCodeTransactionConflict
}

func IsNotSupportedErr(e error) bool {
	ve, ok := e.(*Error)
	if !ok {
		return false
	}
	return ve.Code == consts.ErrCodeNotSupported
}
