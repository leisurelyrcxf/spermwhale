package logical

import (
	"context"
	"sync"
	"time"

	"github.com/leisurelyrcxf/spermwhale/types/basic"

	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/topo"
)

const (
	maxRetry = 60
)

type Oracle struct {
	counter   basic.AtomicUint64
	persisted basic.AtomicUint64
	store     *topo.Store

	allocInAdvance uint64
	sync.Mutex
}

func NewOracle(allocInAdvance uint64, c *topo.Store) (*Oracle, error) {
	val, err := c.LoadTimestamp()
	if err != nil {
		return nil, err
	}
	return &Oracle{
		counter:   basic.NewAtomicUint64(val + 1),
		persisted: basic.NewAtomicUint64(val + 1),
		store:     c,

		allocInAdvance: allocInAdvance,
	}, nil
}

func (o *Oracle) FetchTimestamp(_ context.Context) (uint64, error) {
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

func (o *Oracle) Close() error {
	return o.store.Close()
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
		err := o.store.UpdateTimestamp(newPersisted)
		if err == nil {
			o.persisted.Set(newPersisted)
			return
		}
		if i < maxRetry-1 {
			glog.Warningf("update %v to %d failed: %v, retrying...", o.store.TimestampPath(), newPersisted, err)
		} else {
			glog.Fatalf("update %v to %d failed: %v, retrying...", o.store.TimestampPath(), newPersisted, err)
		}
		time.Sleep(time.Second)
	}
	panic("unreachable code")
}
