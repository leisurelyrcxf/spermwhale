package physical

import (
	"context"
	"time"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

type LoosedPrecisionOracle struct {
	Oracle

	lastTimestamp uint64
}

// This one is used for redis db because zset only has float64 as score type,
// if don't do this, can't handle precision lost....
func NewLoosedPrecisionOracle() *LoosedPrecisionOracle {
	return &LoosedPrecisionOracle{}
}

func (o *LoosedPrecisionOracle) FetchTimestamp(_ context.Context) (uint64, error) {
	o.Lock()
	defer o.Unlock()

	for {
		ts := utils.GetLocalTimestamp()
		ts >>= consts.LoosedOracleDiscardedBits
		ts <<= consts.LoosedOracleDiscardedBits
		if ts-o.lastTimestamp > consts.LoosedOraclePrecision {
			o.lastTimestamp = ts
			return ts, nil
		}
		time.Sleep(consts.LoosedOracleWaitPeriod)
	}
}
