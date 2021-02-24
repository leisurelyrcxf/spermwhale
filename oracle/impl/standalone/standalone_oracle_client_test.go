package standalone

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/models"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestClient_FetchTimestamp(t *testing.T) {
	assert := testifyassert.New(t)

	dataPath := filepath.Join("/tmp/data/", OraclePath)
	if exists(dataPath) {
		if !assert.NoError(os.Remove(dataPath)) {
			return
		}
	}
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

func TestClient_FetchTimestamp2(t *testing.T) {
	assert := testifyassert.New(t)

	dataPath := filepath.Join("/tmp/data/", OraclePath)
	if exists(dataPath) {
		if !assert.NoError(os.Remove(dataPath)) {
			return
		}
	}

	const port = 9999
	modelClient, err := models.NewClient("fs", "/tmp/", "", time.Minute)
	if !assert.NoError(err) {
		return
	}
	server := NewServer(port, 100, modelClient)
	assert.NoError(server.Start())

	var fetchWg sync.WaitGroup
	const threadNum = 3
	var clientErrs [threadNum]error
	var maxSeenTimestamps [threadNum]uint64
	for i := 0; i < threadNum; i++ {
		fetchWg.Add(1)

		go func(i int) {
			defer fetchWg.Done()

			client, err := NewClient(fmt.Sprintf("localhost:%d", port))
			if !assert.NoError(err) {
				clientErrs[i] = err
				return
			}
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			for {
				ts, err := client.FetchTimestamp(ctx)
				if err != nil {
					break
				}
				assert.Greater(ts, maxSeenTimestamps[i])
				maxSeenTimestamps[i] = ts
			}
		}(i)
	}

	time.Sleep(6 * time.Second)
	server.Stop()
	fetchWg.Wait()
	var maxSeenTS uint64
	for i := 0; i < threadNum; i++ {
		assert.NoError(clientErrs[i])
		if maxSeenTimestamps[i] > maxSeenTS {
			maxSeenTS = maxSeenTimestamps[i]
		}
	}

	{
		modelClient, err := models.NewClient("fs", "/tmp/", "", time.Minute)
		if !assert.NoError(err) {
			return
		}
		server2 := NewServer(port, 100, modelClient)
		assert.NoError(server2.Start())
		defer server2.Stop()

		client, clientErr := NewClient(fmt.Sprintf("localhost:%d", port))
		if !assert.NoError(clientErr) {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		newTS, err := client.FetchTimestamp(ctx)
		assert.NoError(err)
		assert.Greater(newTS, maxSeenTS)
	}
}

func exists(file string) bool {
	_, err := os.Stat(file)
	if err == nil {
		return true
	}
	return !os.IsNotExist(err)
}
