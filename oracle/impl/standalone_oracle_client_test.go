package impl

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/topo"

	"github.com/leisurelyrcxf/spermwhale/topo/client"

	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"

	"github.com/leisurelyrcxf/spermwhale/oracle/impl/logical"
	testifyassert "github.com/stretchr/testify/assert"
)

const rounds = 10

func newServer(assert *testifyassert.Assertions, port int, logicalOracle bool, clearDataForLogical bool) *Server {
	cli, err := client.NewClient("fs", "/tmp/", "", time.Minute)
	if !assert.NoError(err) {
		return nil
	}
	store := topo.NewStore(cli, "test_cluster")
	if clearDataForLogical {
		if !assert.NoError(store.DeleteTimestamp()) {
			return nil
		}
	}
	if logicalOracle {
		o, err := logical.NewOracle(100, store)
		if !assert.NoError(err) {
			return nil
		}
		return NewServer(port, o, store)
	}
	return NewServer(port, physical.NewOracle(), store)
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
	server := newServer(assert, port, logicalOracle, true)
	assert.NoError(server.Start())
	defer server.Stop()
	var serverAddr = fmt.Sprintf("localhost:%d", port)

	cli, err := NewClient(serverAddr)
	if !assert.NoError(err) {
		return
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	ts1, err := cli.FetchTimestamp(ctx)
	if !assert.Equal(uint64(1), ts1) {
		return
	}

	ts2, err := cli.FetchTimestamp(ctx)
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
	server1 := newServer(assert, port, logicalOracle, true)
	if !assert.NotNil(server1) {
		return
	}
	assert.NoError(server1.Start())

	var fetchWg sync.WaitGroup
	const threadNum = 100
	var clientErrs [threadNum]error
	var maxSeenTimestamps [threadNum]uint64
	var timestamps [threadNum][]uint64
	for i := 0; i < threadNum; i++ {
		fetchWg.Add(1)

		go func(i int) {
			defer fetchWg.Done()

			cli, err := NewClient(fmt.Sprintf("localhost:%d", port))
			if !assert.NoError(err) {
				clientErrs[i] = err
				return
			}
			defer cli.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			for {
				ts, err := cli.FetchTimestamp(ctx)
				if err != nil {
					break
				}
				assert.Greater(ts, maxSeenTimestamps[i])
				maxSeenTimestamps[i] = ts
				timestamps[i] = append(timestamps[i], ts)
			}
		}(i)
	}

	rand.Seed(time.Now().UnixNano())
	time.Sleep(time.Duration(rand.Intn(3000))*time.Millisecond + time.Second*3)
	server1.Stop()
	fetchWg.Wait()
	var (
		maxSeenTS     uint64
		allTimestamps []uint64
	)
	for i := 0; i < threadNum; i++ {
		assert.NoError(clientErrs[i])
		if maxSeenTimestamps[i] > maxSeenTS {
			maxSeenTS = maxSeenTimestamps[i]
		}
		allTimestamps = append(allTimestamps, timestamps[i]...)
	}
	allTimestampsInts := make([]int, len(allTimestamps))
	for i, v := range allTimestamps {
		allTimestampsInts[i] = int(v)
	}
	sort.Ints(allTimestampsInts)
	for i := 0; i < len(allTimestampsInts)-1; i++ {
		assert.GreaterOrEqual(allTimestampsInts[i+1], allTimestampsInts[i])
		assert.Greater(allTimestampsInts[i+1], allTimestampsInts[i])
	}

	{
		server2 := newServer(assert, port, logicalOracle, false)
		assert.NoError(server2.Start())
		defer server2.Stop()

		cli, clientErr := NewClient(fmt.Sprintf("localhost:%d", port))
		if !assert.NoError(clientErr) {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		newTS, err := cli.FetchTimestamp(ctx)
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
