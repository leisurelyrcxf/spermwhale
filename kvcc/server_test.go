package kvcc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/kv/impl/memory"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"
	"github.com/leisurelyrcxf/spermwhale/topo"
	"github.com/leisurelyrcxf/spermwhale/topo/client"
	"github.com/leisurelyrcxf/spermwhale/types"

	testifyassert "github.com/stretchr/testify/assert"
)

func newTestServer(assert *testifyassert.Assertions, port int) (server *Server) {
	cli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
	if !assert.NoError(err) {
		return nil
	}
	return NewServer(port, memory.NewMemoryDB(),
		types.NewTabletTxnManagerConfig(
			types.NewTabletTxnConfig(time.Second).WithMaxClockDrift(time.Nanosecond),
			types.DefaultReadModifyWriteQueueCfg,
		).WithTest(),
		1, topo.NewStore(cli, "test_cluster"))
}

func newReadOption(version uint64) types.KVCCReadOption {
	return *types.NewKVCCReadOption(version)
}

func TestKV_Get(t *testing.T) {
	assert := testifyassert.New(t)

	const port = 9999
	server := newTestServer(assert, port)
	assert.NoError(server.Start())
	defer server.Close()
	var serverAddr = fmt.Sprintf("localhost:%d", port)

	client, err := NewClient(serverAddr)
	if !assert.NoError(err) {
		return
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	_, err = client.Get(ctx, "k1", newReadOption(1))
	assert.Error(err)

	base := physical.NewOracle().MustFetchTimestamp()
	ts1 := base - uint64(time.Millisecond)*10
	ts2 := base

	if !assert.NoError(client.Set(ctx, "k1", types.NewValue([]byte("v1"), ts1).WithInternalVersion(1), types.NewKVCCWriteOption())) {
		return
	}
	if !assert.NoError(client.Set(ctx, "k1", types.NewValue([]byte("v2"), ts2).WithInternalVersion(1), types.NewKVCCWriteOption())) {
		return
	}

	{
		vv, err := client.Get(ctx, "k1", newReadOption(base-uint64(time.Millisecond)*5))
		if !assert.NoError(err) {
			return
		}
		assert.Equal(ts1, vv.Version)
		assert.Equal([]byte("v1"), vv.V)
		assert.Equal(true, vv.IsDirty())
	}

	{
		vv, err := client.Get(ctx, "k1", newReadOption(base))
		if !assert.NoError(err) {
			return
		}
		assert.Equal(ts2, vv.Version)
		assert.Equal([]byte("v2"), vv.V)
		assert.Equal(true, vv.IsDirty())
	}

	{
		vv, err := client.Get(ctx, "k1", newReadOption(physical.NewOracle().MustFetchTimestamp()))
		if !assert.NoError(err) {
			return
		}
		assert.Equal(ts2, vv.Version)
		assert.Equal([]byte("v2"), vv.V)
		assert.Equal(true, vv.IsDirty())
	}
}

func TestKV_Get2(t *testing.T) {
	assert := testifyassert.New(t)

	const port = 9999
	server := newTestServer(assert, port)
	assert.NoError(server.Start())
	defer server.Close()
	var serverAddr = fmt.Sprintf("localhost:%d", port)

	cli, err := NewClient(serverAddr)
	if !assert.NoError(err) {
		return
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	_, err = cli.Get(ctx, "k1", newReadOption(1))
	assert.Error(err)

	base := physical.NewOracle().MustFetchTimestamp()
	ts1 := base - uint64(time.Millisecond)*10
	ts2 := base

	if !assert.NoError(cli.Set(ctx, "k1", types.NewValue([]byte("v2"), ts2).WithInternalVersion(1), types.NewKVCCWriteOption())) {
		return
	}
	if !assert.NoError(cli.Set(ctx, "k1", types.NewValue([]byte("v1"), ts1).WithInternalVersion(1), types.NewKVCCWriteOption())) {
		return
	}

	{
		vv, err := cli.Get(ctx, "k1", newReadOption(base-uint64(time.Millisecond)*5))
		if !assert.NoError(err) {
			return
		}
		assert.Equal(ts1, vv.Version)
		assert.Equal([]byte("v1"), vv.V)
		assert.Equal(true, vv.IsDirty())
	}

	{
		vv, err := cli.Get(ctx, "k1", newReadOption(base))
		if !assert.NoError(err) {
			return
		}
		assert.Equal(ts2, vv.Version)
		assert.Equal([]byte("v2"), vv.V)
		assert.Equal(true, vv.IsDirty())
	}

	{
		vv, err := cli.Get(ctx, "k1", newReadOption(base+uint64(time.Millisecond)*15))
		if !assert.NoError(err) {
			return
		}
		assert.Equal(ts2, vv.Version)
		assert.Equal([]byte("v2"), vv.V)
		assert.Equal(true, vv.IsDirty())
	}

	err = cli.Set(ctx, "k1", types.NewValue([]byte("v5"), base+uint64(time.Millisecond)*10).WithInternalVersion(1), types.NewKVCCWriteOption())
	assert.Error(err)
	assert.Contains(err.Error(), errors.ErrWriteReadConflict.Msg)
}
