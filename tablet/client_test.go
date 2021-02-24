package tablet

import (
	"context"
	"fmt"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestKV_Get(t *testing.T) {
	assert := testifyassert.New(t)

	const port = 9999
	server := NewServer(port)
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

	if !assert.NoError(client.Set(ctx, "k1", "v1", 1, true)) {
		return
	}
	if !assert.NoError(client.Set(ctx, "k1", "v2", 2, false)) {
		return
	}

	{
		vv, err := client.Get(ctx, "k1", 1)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(uint64(1), vv.Version)
		assert.Equal("v1", vv.V)
		assert.Equal(true, vv.WriteIntent)
	}

	{
		vv, err := client.Get(ctx, "k1", 2)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(uint64(2), vv.Version)
		assert.Equal("v2", vv.V)
		assert.Equal(false, vv.WriteIntent)
	}
}

func TestKV_Get2(t *testing.T) {
	assert := testifyassert.New(t)

	const port = 9999
	server := NewServer(port)
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

	if !assert.NoError(client.Set(ctx, "k1", "v2", 2, false)) {
		return
	}
	if !assert.NoError(client.Set(ctx, "k1", "v1", 1, true)) {
		return
	}

	{
		vv, err := client.Get(ctx, "k1", 1)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(uint64(1), vv.Version)
		assert.Equal("v1", vv.V)
		assert.Equal(true, vv.WriteIntent)
	}

	{
		vv, err := client.Get(ctx, "k1", 6)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(uint64(2), vv.Version)
		assert.Equal("v2", vv.V)
		assert.Equal(false, vv.WriteIntent)
	}

	err = client.Set(ctx, "k1", "v5", 5, true)
	assert.Error(err)
	assert.Contains(err.Error(), ErrMsgVersionConflict)
}
