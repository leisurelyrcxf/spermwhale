package utils

import (
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"
)

func CheckTooStale(version uint64, staleWriteThreshold time.Duration) error {
	currentTS := getLocalTimestamp()
	if version < currentTS && currentTS-version > uint64(staleWriteThreshold) {
		return errors.Annotatef(errors.ErrStaleWrite,
			"age(%s) > stale_thr(%s)", time.Duration(currentTS-version), staleWriteThreshold)
	}
	return nil
}

func IsTooStale(version uint64, staleWriteThreshold time.Duration) bool {
	currentTS := getLocalTimestamp()
	return version < currentTS && currentTS-version > uint64(staleWriteThreshold)
}

func getLocalTimestamp() uint64 {
	return uint64(time.Now().UnixNano())
}
