package tablet

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/proto/kvpb"
	"google.golang.org/grpc"

	testifyassert "github.com/stretchr/testify/assert"
)

func StartTabletServer(t *testing.T) (stopper func()) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 9999))
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
		return nil
	}

	grpcServer := grpc.NewServer()
	kvpb.RegisterKVServer(grpcServer, NewKV())

	done := make(chan struct{})

	go func() {
		defer close(done)

		if err := grpcServer.Serve(lis); err != nil {
			t.Fatalf("serve failed: %v", err)
		} else {
			t.Logf("server terminated successfully")
		}
	}()

	return func() {
		grpcServer.Stop()
		<-done
	}
}

func TestKV_Get(t *testing.T) {
	assert := testifyassert.New(t)

	stopper := StartTabletServer(t)
	if !assert.NotNil(stopper) {
		return
	}
	defer stopper()

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
		assert.Equal("v1", vv.Value.V)
		assert.Equal(true, vv.Value.M.WriteIntent)
	}

	{
		vv, err := client.Get(ctx, "k1", 2)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(uint64(2), vv.Version)
		assert.Equal("v2", vv.Value.V)
		assert.Equal(false, vv.Value.M.WriteIntent)
	}
}

func TestKV_Get2(t *testing.T) {
	assert := testifyassert.New(t)

	stopper := StartTabletServer(t)
	if !assert.NotNil(stopper) {
		return
	}
	defer stopper()

	const serverAddr = "localhost:9999"
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
		assert.Equal("v1", vv.Value.V)
		assert.Equal(true, vv.Value.M.WriteIntent)
	}

	{
		vv, err := client.Get(ctx, "k1", 6)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(uint64(2), vv.Version)
		assert.Equal("v2", vv.Value.V)
		assert.Equal(false, vv.Value.M.WriteIntent)
	}

	err = client.Set(ctx, "k1", "v5", 5, true)
	assert.Error(err)
	t.Logf("set error: %v", err)
}
