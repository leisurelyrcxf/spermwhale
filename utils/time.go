package utils

import (
	"context"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"

	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/oracle"

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

func MustFetchTimestamp(oracleFac oracle.Factory) uint64 {
	ts, err := FetchTimestampWithRetry(oracleFac)
	if err != nil {
		glog.Fatalf("failed to fetch timestamp: '%v'", err)
	}
	return ts
}

func FetchTimestampWithRetry(oracleFac oracle.Factory) (ts uint64, err error) {
	parentCtx, parentCancel := context.WithTimeout(context.Background(), time.Second*30)
	defer parentCancel()

	for parentCtx.Err() == nil {
		ora := oracleFac.GetOracle()
		if ora == nil {
			err = errors.ErrCantGetOracle
			glog.Warningf("oracleFac.GetOracle() == nil")
			continue
		}
		ts, err = ora.FetchTimestamp(parentCtx)
		if err == nil {
			return ts, nil
		}
		glog.Warningf("fetch timestamp failed: '%v'", err)
		time.Sleep(time.Second)
	}
	assert.Must(err != nil)
	return ts, err
}
