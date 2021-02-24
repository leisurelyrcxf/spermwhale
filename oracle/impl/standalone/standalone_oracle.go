package standalone

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/models"
	"github.com/leisurelyrcxf/spermwhale/models/common"
	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"
	"github.com/leisurelyrcxf/spermwhale/proto/oraclepb"
	"github.com/leisurelyrcxf/spermwhale/sync2"
)

const (
	OraclePath = "spermwhale/ts_oracle"

	maxRetry = 60
)

type Oracle struct {
	oraclepb.UnimplementedOracleServer

	counter   sync2.AtomicUint64
	persisted sync2.AtomicUint64
	client    models.Client

	allocInAdvance uint64
	sync.Mutex
}

func NewOracle(allocInAdvance uint64, c models.Client) (*Oracle, error) {
	val, err := c.Read(OraclePath, true)
	if err != nil && err != common.ErrKeyNotExists {
		return nil, err
	}
	var next uint64 = 0
	if err == nil {
		persisted, err := strconv.ParseUint(string(val), 10, 64)
		if err != nil {
			return nil, err
		}
		next = persisted + 1
	}
	return &Oracle{
		counter:   sync2.NewAtomicUint64(next),
		persisted: sync2.NewAtomicUint64(next),
		client:    c,

		allocInAdvance: allocInAdvance,
	}, nil
}

func (o *Oracle) Fetch(context.Context, *oraclepb.FetchRequest) (*oraclepb.FetchResponse, error) {
	ts, err := o.FetchTimestamp()
	if err != nil {
		return &oraclepb.FetchResponse{
			Err: &commonpb.Error{
				Code: consts.ErrCodeOther,
				Msg:  err.Error(),
			},
		}, nil
	}
	return &oraclepb.FetchResponse{Ts: ts}, nil
}

func (o *Oracle) FetchTimestamp() (uint64, error) {
	for {
		if current, persisted := o.counter.Get(), o.persisted.Get(); current+o.allocInAdvance/3 < persisted {
			if o.counter.CompareAndSwap(current, current+1) {
				return current + 1, nil
			}
		} else {
			o.alloc(current)
		}
	}
}

func (o *Oracle) alloc(counter uint64) {
	o.Lock()
	defer o.Unlock()

	oldPersisted := o.persisted.Get()
	if counter+o.allocInAdvance/3 < oldPersisted {
		return
	}
	newPersisted := oldPersisted + o.allocInAdvance
	for i := 0; i < maxRetry; i++ {
		if err := o.client.Update(OraclePath, []byte(strconv.FormatUint(newPersisted, 10))); err != nil {
			if i < maxRetry-1 {
				glog.Warningf("update %v to %d failed: %v, retrying...", OraclePath, newPersisted, err)
			} else {
				glog.Fatalf("update %v to %d failed: %v, retrying...", OraclePath, newPersisted, err)
			}
			time.Sleep(time.Second)
			continue
		}
	}
	o.persisted.Set(newPersisted)
}
