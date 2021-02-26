package physical

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
)

type Oracle struct {
	sync.Mutex
}

func NewOracle() *Oracle {
	return &Oracle{}
}

func (o *Oracle) FetchTimestamp(_ context.Context) (uint64, error) {
	o.Lock()
	defer o.Unlock()

	i := time.Now().UnixNano()
	if i < 0 {
		return 0, fmt.Errorf("time.Now().UnixNano() < 0 ")
	}
	return uint64(i), nil
}

func (o *Oracle) MustFetchTimestamp() uint64 {
	ts, err := o.FetchTimestamp(context.Background())
	if err != nil {
		glog.Fatalf("can't fetch timestamp: %v", err)
	}
	return ts
}

func (o *Oracle) IsTooStale(ts uint64, stalePeriod time.Duration) bool {
	currentTS := o.MustFetchTimestamp()
	return ts < currentTS && currentTS-ts > uint64(stalePeriod)
}
