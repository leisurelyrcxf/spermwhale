package errors

import (
	"errors"
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/types"

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
	if ve, ok := err.(*types.Error); ok {
		return &types.Error{
			Code: ve.Code,
			Msg:  ve.Msg + ", " + fmt.Sprintf(format, args...),
		}
	}
	return New(err.Error() + ", " + fmt.Sprintf(format, args...))
}
