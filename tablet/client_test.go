package tablet

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"

	"github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/leisurelyrcxf/spermwhale/models"
	"github.com/leisurelyrcxf/spermwhale/models/client"
	"github.com/leisurelyrcxf/spermwhale/types"

	testifyassert "github.com/stretchr/testify/assert"
)

func newServer(assert *testifyassert.Assertions, port int) (server *Server) {
	cli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
	if !assert.NoError(err) {
		return nil
	}
	return NewServer(time.Second, time.Nanosecond, 1, port, models.NewStore(cli, "test_cluster"))
}

func TestKV_Get(t *testing.T) {
	assert := testifyassert.New(t)

	const port = 9999
	server := newServer(assert, port)
	assert.NoError(server.Start())
	defer server.Stop()
	var serverAddr = fmt.Sprintf("localhost:%d", port)

	client, err := NewClient(serverAddr)
	if !assert.NoError(err) {
		return
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	_, err = client.Get(ctx, "k1", 0)
	assert.Error(err)

	base := physical.NewOracle().MustFetchTimestamp()
	ts1 := base - uint64(time.Millisecond)*10
	ts2 := base

	if !assert.NoError(client.Set(ctx, "k1", types.NewValue([]byte("v1"), ts1, true), types.WriteOption{})) {
		return
	}
	if !assert.NoError(client.Set(ctx, "k1", types.NewValue([]byte("v2"), ts2, false), types.WriteOption{})) {
		return
	}

	{
		vv, err := client.Get(ctx, "k1", base-uint64(time.Millisecond)*5)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(ts1, vv.Version)
		assert.Equal([]byte("v1"), vv.V)
		assert.Equal(true, vv.WriteIntent)
	}

	{
		vv, err := client.Get(ctx, "k1", base)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(ts2, vv.Version)
		assert.Equal([]byte("v2"), vv.V)
		assert.Equal(false, vv.WriteIntent)
	}

	{
		vv, err := client.Get(ctx, "k1", physical.NewOracle().MustFetchTimestamp())
		if !assert.NoError(err) {
			return
		}
		assert.Equal(ts2, vv.Version)
		assert.Equal([]byte("v2"), vv.V)
		assert.Equal(false, vv.WriteIntent)
	}
}

func TestKV_Get2(t *testing.T) {
	assert := testifyassert.New(t)

	const port = 9999
	server := newServer(assert, port)
	assert.NoError(server.Start())
	defer server.Stop()
	var serverAddr = fmt.Sprintf("localhost:%d", port)

	client, err := NewClient(serverAddr)
	if !assert.NoError(err) {
		return
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	_, err = client.Get(ctx, "k1", 0)
	assert.Error(err)

	base := physical.NewOracle().MustFetchTimestamp()
	ts1 := base - uint64(time.Millisecond)*10
	ts2 := base

	if !assert.NoError(client.Set(ctx, "k1", types.NewValue([]byte("v2"), ts2, false), types.WriteOption{})) {
		return
	}
	if !assert.NoError(client.Set(ctx, "k1", types.NewValue([]byte("v1"), ts1, true), types.WriteOption{})) {
		return
	}

	{
		vv, err := client.Get(ctx, "k1", base-uint64(time.Millisecond)*5)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(ts1, vv.Version)
		assert.Equal([]byte("v1"), vv.V)
		assert.Equal(true, vv.WriteIntent)
	}

	{
		vv, err := client.Get(ctx, "k1", base)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(ts2, vv.Version)
		assert.Equal([]byte("v2"), vv.V)
		assert.Equal(false, vv.WriteIntent)
	}

	{
		vv, err := client.Get(ctx, "k1", base+uint64(time.Millisecond)*15)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(ts2, vv.Version)
		assert.Equal([]byte("v2"), vv.V)
		assert.Equal(false, vv.WriteIntent)
	}

	err = client.Set(ctx, "k1", types.NewValue([]byte("v5"), base+uint64(time.Millisecond)*10, true), types.WriteOption{})
	assert.Error(err)
	assert.Contains(err.Error(), consts.ErrVersionConflict.Msg)
}
