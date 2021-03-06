package errors

import (
	"fmt"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"
)

var (
	mustRollbackGetErrs = map[int32]struct{}{
		consts.ErrCodeWriteReadConflict:         {},
		consts.ErrCodeStaleWrite:                {},
		consts.ErrCodeReadAfterWriteFailed:      {},
		consts.ErrCodeWriteIntentQueueFull:      {},
		consts.ErrCodeReadModifyWriteWaitFailed: {},
		consts.ErrCodeReadModifyWriteQueueFull:  {},
	}

	mustRollbackSetErrs = map[int32]struct{}{
		consts.ErrCodeWriteReadConflict:                {},
		consts.ErrCodeStaleWrite:                       {},
		consts.ErrCodeWriteKeyAfterTabletTxnRollbacked: {},
		consts.ErrCodeTabletWriteTransactionNotFound:   {},
		consts.ErrCodeTimestampCacheWriteQueueFull:     {},
		consts.ErrCodeTabletTxnSetFailedKeyNotFound:    {},
	}

	retryableTxnErrs = mergeCodeSets(mustRollbackGetErrs, mustRollbackSetErrs, map[int32]struct{}{
		consts.ErrCodeReadUncommittedDataPrevTxnStateUndetermined: {},
		consts.ErrCodeReadUncommittedDataPrevTxnAborted:           {},
		consts.ErrCodeTxnRollbacking:                              {},
		consts.ErrCodeTxnRollbacked:                               {},
		consts.ErrCodeSnapshotReadRetriedTooManyTimes:             {},
		consts.ErrCodeMinAllowedSnapshotVersionViolated:           {},
	})
)

func mergeCodeSets(codeSets ...map[int32]struct{}) map[int32]struct{} {
	m := make(map[int32]struct{})
	for _, codeSet := range codeSets {
		for code := range codeSet {
			m[code] = struct{}{}
		}
	}
	return m
}

func in(code int32, codes map[int32]struct{}) bool {
	_, ok := codes[code]
	return ok
}

type ErrorKey struct {
	Code    int32
	SubCode int32
}

var AllErrors = make(map[ErrorKey]*Error, 256)

func registerErr(e *Error) *Error {
	assert.Must(e.Code != consts.ErrCodeUnknown)
	assert.Must(e.SubCode != consts.ErrSubCodeUnknown)
	ek := e.Key()
	if _, ok := AllErrors[ek]; ok {
		glog.Fatalf("error %v already registered", e)
	}
	AllErrors[ek] = e
	return e
}

type Error struct {
	Code    int32
	SubCode int32
	Msg     string
}

func NewErrorFromPB(x *commonpb.Error) error {
	if x == nil {
		return nil
	}
	return &Error{
		Code:    x.Code,
		SubCode: x.SubCode,
		Msg:     x.Msg,
	}
}

func (e *Error) ToPB() *commonpb.Error {
	if e == nil {
		return nil
	}
	return &commonpb.Error{
		Code:    e.Code,
		SubCode: e.SubCode,
		Msg:     e.Msg,
	}
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%v, err_code:%v", e.Msg, e.Code)
}

func (e *Error) Key() ErrorKey {
	return ErrorKey{
		Code:    e.Code,
		SubCode: e.SubCode,
	}
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
			Code:    e.Code,
			SubCode: e.SubCode,
			Msg:     e.Msg,
		}
	}
	return &commonpb.Error{
		Code:    consts.ErrCodeUnknown,
		SubCode: consts.ErrSubCodeUnknown,
		Msg:     e.Error(),
	}
}

func IsMustRollbackGetErr(e error) bool {
	return in(GetErrorCode(e), mustRollbackGetErrs)
}

func IsMustRollbackSetErr(e error) bool {
	return in(GetErrorCode(e), mustRollbackSetErrs)
}

func IsNotExistsErr(e error) bool {
	return GetErrorCode(e) == consts.ErrCodeKeyOrVersionNotExists
}

func IsNotExistsErrEx(e error, subCode *int32) bool {
	var code int32
	code, *subCode = GetErrorCodes(e)
	//assert.Must(!notExists || sub != 0)
	return code == consts.ErrCodeKeyOrVersionNotExists
}

func GetNotExistsErrForAborted(cleared uint8) *Error {
	return KeyOrVersionNotExistErrors[consts.ErrSubCodeKeyOrVersionNotExistsExistsInDBButRollbacking+cleared]
}

func GetReadUncommittedDataOfAbortedTxn(cleared uint8) *Error {
	return ReadUncommittedDataPrevTxnAbortedErrors[consts.ErrSubCodeReadUncommittedDataPrevTxnRollbacking+cleared]
}

func IsRetryableTransactionErr(e error) bool {
	return in(GetErrorCode(e), retryableTxnErrs)
}

func IsRetryableTransactionManagerErr(e error) bool {
	return false
	//return GetErrorCode(e) == consts.ErrCodeTransactionAlreadyExists
}

func IsMustRollbackWriteKeyErr(e error) bool {
	code := GetErrorCode(e)
	return code == consts.ErrCodeWriteReadConflict ||
		code == consts.ErrCodeStaleWrite
}

func IsQueueFullErr(e error) bool {
	code := GetErrorCode(e)
	return code == consts.ErrCodeReadModifyWriteQueueFull || code == consts.ErrCodeWriteIntentQueueFull
}

func IsNotSupportedErr(e error) bool {
	code := GetErrorCode(e)
	return code == consts.ErrCodeNotSupported
}

func IsSnapshotReadTabletErr(err error) bool {
	code := GetErrorCode(err)
	return code == consts.ErrCodeSnapshotReadRetriedTooManyTimes ||
		code == consts.ErrCodeMinAllowedSnapshotVersionViolated
}

func IsErrType(err error, code int32) bool {
	return GetErrorCode(err) == code
}

func ReplaceErr(original, replacement error) error {
	if original == nil {
		return replacement
	}
	return Annotatef(replacement, original.Error())
}

func GetErrorCode(e error) int32 {
	if ve, ok := e.(*Error); ok && ve != nil {
		return ve.Code
	}
	if ce, ok := e.(*commonpb.Error); ok && ce != nil {
		return ce.Code
	}
	return consts.ErrCodeUnknown
}

func GetErrorCodes(e error) (code int32, subCode int32) {
	if ve, ok := e.(*Error); ok && ve != nil {
		return ve.Code, ve.SubCode
	}
	if ce, ok := e.(*commonpb.Error); ok && ce != nil {
		return ce.Code, ce.SubCode
	}
	return consts.ErrCodeUnknown, consts.ErrSubCodeUnknown
}

func SetErrorCode(e error, code int32) {
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

func GetErrorKey(e error) ErrorKey {
	if ve, ok := e.(*Error); ok && ve != nil {
		return ve.Key()
	}
	if ce, ok := e.(*commonpb.Error); ok && ce != nil {
		return ErrorKey{
			Code:    ce.Code,
			SubCode: ce.SubCode,
		}
	}
	return ErrorKey{
		Code: consts.ErrCodeUnknown,
	}
}

func CASErrorCode(e error, oldCode, newCode int32) {
	assert.Must(oldCode != consts.ErrCodeUnknown)

	if GetErrorCode(e) == oldCode {
		SetErrorCode(e, newCode)
	}
}

func CASError(e error, oldCode int32, newErr error) error {
	assert.Must(oldCode != consts.ErrCodeUnknown)

	if GetErrorCode(e) == oldCode {
		return newErr
	}
	return e
}

func CASError2(e error, exp error, newErr error) error {
	if e == exp {
		return newErr
	}
	return e
}
