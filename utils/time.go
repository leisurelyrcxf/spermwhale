package utils

import (
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"
)

func CheckTooStale(version uint64, staleWriteThreshold time.Duration) error {
	currentTS := GetLocalTimestamp()
	if version < currentTS && currentTS-version > uint64(staleWriteThreshold) {
		return errors.Annotatef(errors.ErrStaleWrite,
			"age(%s) > stale_thr(%s)", time.Duration(currentTS-version), staleWriteThreshold)
	}
	return nil
}

func IsTooStale(version uint64, staleWriteThreshold time.Duration) bool {
	currentTS := GetLocalTimestamp()
	return version < currentTS && currentTS-version > uint64(staleWriteThreshold)
}

func GetLocalTimestamp() uint64 {
	return uint64(time.Now().UnixNano())
}
