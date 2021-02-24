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

	if !assert.NoError(client.Set(ctx, "k1", "v1", 10, true)) {
		return
	}
	vv, err := client.Get(ctx, "k1", 10)
	if !assert.NoError(err) {
		return
	}

	assert.Equal(uint64(10), vv.Version)
	assert.Equal("v1", vv.Value.V)
	assert.Equal(true, vv.Value.M.WriteIntent)
}
