package local

import "github.com/leisurelyrcxf/spermwhale/sync2"

type Oracle struct {
	c sync2.AtomicUint64
}

func NewOracle() *Oracle {
	return &Oracle{c: sync2.NewAtomicUint64(0)}
}

func (o *Oracle) FetchTimestamp() (uint64, error) {
	return o.c.Add(1), nil
}
