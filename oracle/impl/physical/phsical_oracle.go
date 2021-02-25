package physical

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"
)

type Oracle struct {
}

func NewOracle() *Oracle {
	return &Oracle{}
}

func (o *Oracle) FetchTimestamp(_ context.Context) (uint64, error) {
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
