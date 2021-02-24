package consts

import "fmt"

var (
	ErrTxnConflict = fmt.Errorf("transaction conflict")
)

const (
	ErrCodeOther = 1111
)
