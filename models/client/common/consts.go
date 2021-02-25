package common

import "errors"

var (
	ErrKeyNotExists     = errors.New("key not exists")
	ErrKeyAlreadyExists = errors.New("key already exists")
)
