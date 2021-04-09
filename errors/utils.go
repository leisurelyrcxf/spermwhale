package errors

import (
	"errors"
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"

	"github.com/leisurelyrcxf/spermwhale/utils/trace"
)

var TraceEnabled = true

type TracedError struct {
	Stack trace.Stack
	Cause error
}

func (e *TracedError) Error() string {
	return e.Cause.Error()
}

func New(s string) error {
	return errors.New(s)
}

func Trace(err error) error {
	if err == nil || !TraceEnabled {
		return err
	}
	_, ok := err.(*TracedError)
	if ok {
		return err
	}
	return &TracedError{
		Stack: trace.TraceN(1, 32),
		Cause: err,
	}
}

func Errorf(format string, v ...interface{}) error {
	err := fmt.Errorf(format, v...)
	if !TraceEnabled {
		return err
	}
	return &TracedError{
		Stack: trace.TraceN(1, 32),
		Cause: err,
	}
}

func Stack(err error) trace.Stack {
	if err == nil {
		return nil
	}
	e, ok := err.(*TracedError)
	if ok {
		return e.Stack
	}
	return nil
}

func Cause(err error) error {
	for err != nil {
		e, ok := err.(*TracedError)
		if ok {
			err = e.Cause
		} else {
			return err
		}
	}
	return nil
}

func Equal(err1, err2 error) bool {
	e1 := Cause(err1)
	e2 := Cause(err2)
	if e1 == e2 {
		return true
	}
	if e1 == nil || e2 == nil {
		return e1 == e2
	}
	return e1.Error() == e2.Error()
}

func NotEqual(err1, err2 error) bool {
	return !Equal(err1, err2)
}

func Wrap(err, other error) error {
	if err == nil {
		return other
	}
	if other == nil {
		return err
	}
	return Errorf("%v: %v", err, other)
}

func Annotatef(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	if ve, ok := err.(*Error); ok {
		return &Error{
			Code:    ve.Code,
			SubCode: ve.SubCode,
			Msg:     trimMsg(ve.Msg) + ": " + fmt.Sprintf(format, args...),
		}
	}
	if ce, ok := err.(*commonpb.Error); ok {
		return &commonpb.Error{
			Code:    ce.Code,
			SubCode: ce.SubCode,
			Msg:     trimMsg(ce.Msg) + ": " + fmt.Sprintf(format, args...),
		}
	}
	return errors.New(trimMsg(err.Error()) + ": " + fmt.Sprintf(format, args...))
}

func trimMsg(msg string) string {
	if len(msg) > 1024 {
		msg = msg[:1024-256]
	}
	return msg
}
