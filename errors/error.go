package errors

import (
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"
)

var (
	mustRollbackGetErrs = map[int]struct{}{
		consts.ErrCodeWriteReadConflict:      {},
		consts.ErrCodeStaleWrite:             {},
		consts.ErrCodeReadAfterWriteFailed:   {},
		consts.ErrCodeWriteIntentQueueFull:   {},
		consts.ErrCodeReadForWriteWaitFailed: {},
		consts.ErrCodeReadForWriteTooStale:   {},
		consts.ErrCodeReadForWriteQueueFull:  {},
	}

	retryableTxnErrs = mergeSet(mustRollbackGetErrs, map[int]struct{}{
		consts.ErrCodeReadUncommittedDataPrevTxnStateUndetermined: {},
		consts.ErrCodeReadUncommittedDataPrevTxnHasBeenRollbacked: {},
		consts.ErrCodeReadUncommittedDataPrevTxnToBeRollbacked:    {},
	})
)

func mergeSet(codes1, codes2 map[int]struct{}) map[int]struct{} {
	m := make(map[int]struct{})
	for code := range codes1 {
		m[code] = struct{}{}
	}
	for code := range codes2 {
		m[code] = struct{}{}
	}
	return m
}

func in(code int, codes map[int]struct{}) bool {
	_, ok := codes[code]
	return ok
}

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
	return in(GetErrorCode(e), retryableTxnErrs)
}

func IsRetryableTransactionManagerErr(e error) bool {
	code := GetErrorCode(e)
	return code == consts.ErrCodeTransactionAlreadyExists
}

func IsRetryableGetErr(e error) bool {
	code := GetErrorCode(e)
	return code == consts.ErrCodeReadUncommittedDataPrevTxnHasBeenRollbacked
}

func IsMustRollbackGetErr(e error) bool {
	return in(GetErrorCode(e), mustRollbackGetErrs)
}

func IsMustRollbackWriteKeyErr(e error) bool {
	code := GetErrorCode(e)
	return code == consts.ErrCodeWriteReadConflict ||
		code == consts.ErrCodeStaleWrite
}

func IsQueueFullErr(e error) bool {
	code := GetErrorCode(e)
	return code == consts.ErrCodeReadForWriteQueueFull || code == consts.ErrCodeWriteIntentQueueFull
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
