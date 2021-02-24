package standalone

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/models"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestClient_FetchTimestamp(t *testing.T) {
	assert := testifyassert.New(t)

	const port = 9999
	modelClient, err := models.NewClient("fs", "/tmp/", "", time.Minute)
	if !assert.NoError(err) {
		return
	}
	server := NewServer(port, 100, modelClient)
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

	ts1, err := client.FetchTimestamp(ctx)
	assert.Equal(uint64(1), ts1)

	ts2, err := client.FetchTimestamp(ctx)
	assert.Equal(uint64(2), ts2)
}
