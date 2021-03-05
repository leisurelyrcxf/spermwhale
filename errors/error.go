package errors

import (
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/assert"

	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"

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

func NewErrorFromPB(x *commonpb.Error) error {
	if x == nil {
		return nil
	}
	return NewError(int(x.Code), x.Msg)
}

func (e *Error) ToPB() *commonpb.Error {
	if e == nil {
		return nil
	}
	return &commonpb.Error{
		Code: int32(e.Code),
		Msg:  e.Msg,
	}
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%v, err_code:%v", e.Msg, e.Code)
}

func ToPBError(e error) *commonpb.Error {
	if e == nil {
		return nil
	}
	if e, ok := e.(*commonpb.Error); ok {
		return e
	}
	if e, ok := e.(*Error); ok {
		return &commonpb.Error{
			Code: int32(e.Code),
			Msg:  e.Msg,
		}
	}
	return &commonpb.Error{
		Code: consts.ErrCodeUnknown,
		Msg:  e.Error(),
	}
}

func IsNotExistsErr(e error) bool {
	code := GetErrorCode(e)
	return code == consts.ErrCodeKeyNotExists ||
		code == consts.ErrCodeVersionNotExists
}

func IsRetryableTransactionErr(e error) bool {
	code := GetErrorCode(e)
	return code == consts.ErrCodeWriteReadConflict ||
		code == consts.ErrCodeStaleWrite ||
		code == consts.ErrCodeReadAfterWriteFailed ||
		code == consts.ErrCodeReadUncommittedData ||
		code == consts.ErrCodeReadRollbackedData
}

func IsRetryableTransactionManagerErr(e error) bool {
	code := GetErrorCode(e)
	return code == consts.ErrCodeTransactionAlreadyExists
}

func IsRetryableGetErr(e error) bool {
	code := GetErrorCode(e)
	return code == consts.ErrCodeReadRollbackedData
}

func IsMustRollbackGetErr(e error) bool {
	code := GetErrorCode(e)
	return code == consts.ErrCodeReadAfterWriteFailed ||
		code == consts.ErrCodeWriteReadConflict ||
		code == consts.ErrCodeStaleWrite
}

func IsMustRollbackWriteKeyErr(e error) bool {
	code := GetErrorCode(e)
	return code == consts.ErrCodeWriteReadConflict ||
		code == consts.ErrCodeStaleWrite
}

func IsMustRollbackCommitErr(e error) bool {
	code := GetErrorCode(e)
	return code == consts.ErrCodeWriteReadConflict ||
		code == consts.ErrCodeStaleWrite
}

func IsNotSupportedErr(e error) bool {
	code := GetErrorCode(e)
	return code == consts.ErrCodeNotSupported
}

func GetErrorCode(e error) int {
	if ve, ok := e.(*Error); ok && ve != nil {
		return ve.Code
	}
	if ce, ok := e.(*commonpb.Error); ok && ce != nil {
		return int(ce.Code)
	}
	return consts.ErrCodeUnknown
}

func SetErrorCode(e error, code int) {
	if ve, ok := e.(*Error); ok {
		ve.Code = code
		return
	}
	if ce, ok := e.(*commonpb.Error); ok {
		ce.Code = int32(code)
		return
	}
	panic("impossible")
}

func CASErrorCode(e error, oldCode, newCode int) {
	assert.Must(oldCode != consts.ErrCodeUnknown)

	if GetErrorCode(e) == oldCode {
		SetErrorCode(e, newCode)
	}
}
