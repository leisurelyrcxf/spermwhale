package concurrency

import (
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
)

var (
	ErrPrevExists = &errors.Error{
		Code:    consts.ErrCodePrevExists,
		SubCode: 1,
		Msg:     "prev exists",
	}
)
