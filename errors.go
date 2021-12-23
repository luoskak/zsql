package zsql

import "errors"

var (
	ErrInvalidValue = errors.New("invalid value, should be pointer to struct or slice")
	// ErrInvalidTransaction invalid transaction when you are trying to `Commit` or `Rollback`
	ErrInvalidTransaction = errors.New("invalid transaction")
)
