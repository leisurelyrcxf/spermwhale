package memory

import (
	"context"
	"testing"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/leisurelyrcxf/spermwhale/types"
)

func TestMemoryDB(t *testing.T) {
	for i := 0; i < 1000; i++ {
		if !testMemoryDB(t) {
			return
		}
	}
}

func testMemoryDB(t *testing.T) (b bool) {
	assert := testifyassert.New(t)
	db := NewDB()
	ctx := context.Background()

	key := "key1"
	assert.NoError(db.Set(ctx, key, types.IntValue(1).WithVersion(1), types.KVWriteOption{}))
	val, err := db.Get(ctx, key, types.NewKVReadOption(1))
	if !assert.NoError(err) || !assert.Equal(1, val.MustInt()) {
		return
	}
	assert.NoError(db.Set(ctx, key, types.IntValue(2).WithVersion(2), types.KVWriteOption{}))
	val, err = db.Get(ctx, key, types.NewKVReadOption(2))
	if !assert.NoError(err) || !assert.Equal(2, val.MustInt()) {
		return
	}
	assert.NoError(db.Set(ctx, key, types.IntValue(3).WithVersion(3), types.KVWriteOption{}))
	val, err = db.Get(ctx, key, types.NewKVReadOption(3))
	if !assert.NoError(err) || !assert.Equal(3, val.MustInt()) {
		return
	}
	return true
}
