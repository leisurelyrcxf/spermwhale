package impl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"

	"github.com/leisurelyrcxf/spermwhale/models"
	"github.com/leisurelyrcxf/spermwhale/oracle"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl/logical"
	testifyassert "github.com/stretchr/testify/assert"
)

const rounds = 10

func newOracle(assert *testifyassert.Assertions, port int, logicalOracle bool, clearDataForLogical bool) oracle.Oracle {
	if logicalOracle {
		if clearDataForLogical {
			dataPath := filepath.Join("/tmp/data/", logical.OraclePath)
			if exists(dataPath) {
				if !assert.NoError(os.Remove(dataPath)) {
					return nil
				}
			}
		}
		modelClient, err := models.NewClient("fs", "/tmp/", "", time.Minute)
		if !assert.NoError(err) {
			return nil
		}
		o, err := logical.NewOracle(100, modelClient)
		if !assert.NoError(err) {
			return nil
		}
		return o
	}
	return physical.NewOracle()
}

func TestClient_FetchTimestamp(t *testing.T) {
	for i := 0; i < rounds; i++ {
		for _, l := range []bool{true} {
			if !testClientFetchTimestamp(t, l) {
				return
			}
		}
	}
}

func testClientFetchTimestamp(t *testing.T, logicalOracle bool) (b bool) {
	assert := testifyassert.New(t)

	const port = 9999
	server := NewServer(port, newOracle(assert, port, logicalOracle, true))
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
	if !assert.Equal(uint64(1), ts1) {
		return
	}

	ts2, err := client.FetchTimestamp(ctx)
	return assert.Equal(uint64(2), ts2)
}

func TestClient_FetchTimestampLogical(t *testing.T) {
	if !testClientFetchTimestamp2(t, true) {
		return
	}
}

func TestClient_FetchTimestampPhysical(t *testing.T) {
	if !testClientFetchTimestamp2(t, false) {
		return
	}
}

func testClientFetchTimestamp2(t *testing.T, logicalOracle bool) (b bool) {
	assert := testifyassert.New(t)

	const port = 9999
	server := NewServer(port, newOracle(assert, port, logicalOracle, true))
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
		server2 := NewServer(port, newOracle(assert, port, logicalOracle, false))
		assert.NoError(server2.Start())
		defer server2.Stop()

		client, clientErr := NewClient(fmt.Sprintf("localhost:%d", port))
		if !assert.NoError(clientErr) {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		newTS, err := client.FetchTimestamp(ctx)
		if !assert.NoError(err) {
			return
		}
		if !assert.Greater(newTS, maxSeenTS) {
			return
		}
	}

	return true
}

func exists(file string) bool {
	_, err := os.Stat(file)
	if err == nil {
		return true
	}
	return !os.IsNotExist(err)
}
