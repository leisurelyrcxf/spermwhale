package tablet

import (
	"context"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestKV_Get(t *testing.T) {
	assert := testifyassert.New(t)

	const serverAddr = "localhost:9999"
	client, err := NewClient(serverAddr)
	if !assert.NoError(err) {
		return
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	_, err = client.Get(ctx, "k1", 10)
	assert.Error(err)

	if !assert.NoError(client.Set(ctx, "k1", "v1", 10)) {
		return
	}
	vv, err := client.Get(ctx, "k1", 10)
	if !assert.NoError(err) {
		return
	}

	assert.Equal(10, vv.Version)
	assert.Equal("v1", vv.Value)
}
