package physical

import (
	"context"
	"fmt"
	"time"
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
