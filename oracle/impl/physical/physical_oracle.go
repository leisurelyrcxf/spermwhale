package physical

import (
	"context"
	"sync"
	"time"

	"github.com/leisurelyrcxf/spermwhale/utils"

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

	return utils.GetLocalTimestamp(), nil
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
