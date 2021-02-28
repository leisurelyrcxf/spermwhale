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
	for i := 0; i < 30; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
		ts, err = oracleFac.GetOracle().FetchTimestamp(ctx)
		cancel()
		if err == nil {
			return ts, nil
		}
		time.Sleep(time.Second)
	}
	assert.Must(err != nil)
	return ts, err
}
