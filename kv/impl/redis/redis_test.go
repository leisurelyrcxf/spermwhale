package redis

import (
	"context"
	"sync"
	"testing"

	"github.com/leisurelyrcxf/spermwhale/build_opt"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"

	"github.com/leisurelyrcxf/spermwhale/kv"
)

const (
	rounds = 20
)

func TestRedis(t *testing.T) {
	cli := mustNewClient("localhost:6379", "")
	for i := 0; i < 1000; i++ {
		for _, dbug := range []bool{true, false} {
			build_opt.Debug = dbug
			if !kv.TestDB(t, cli) {
				t.Errorf("testRedis failed @round %d, debug: %v", i, dbug)
				return
			}
			t.Logf("testRredis succeeded @round %d, debug: %v", i, dbug)
		}
	}
}

func TestConcurrentInsert(t *testing.T) {
	kv.Testing = true
	for i := 0; i < rounds; i++ {
		if !testConcurrentInsert(t) {
			t.Errorf("testConcurrentInsert failed")
			return
		}
		t.Logf("testConcurrentInsert round %d succeeded", i)
	}
}

func testConcurrentInsert(t *testing.T) (b bool) {
	assert := types.NewAssertion(t)
	cli := mustNewClient("localhost:6379", "")
	const key = "key_update"

	if !assert.NoError(cli.Put(context.Background(), key, types.NewValue([]byte("111"), 1))) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := cli.Set(context.Background(), key, types.NewValue(nil, 1).WithNoWriteIntent(),
				types.NewKVWriteOption()); !errors.AssertNilOrErr(assert, err, errors.ErrVersionAlreadyExists) {
				return
			}
		}()
	}

	wg.Wait()
	val, err := cli.Get(context.Background(), key, types.NewKVReadOption(1).WithExactVersion())
	if !assert.NoError(err) || !assert.True(val.HasWriteIntent()) {
		return
	}
	return true
}

func TestConcurrentUpdate(t *testing.T) {
	for i := 0; i < rounds; i++ {
		if !testConcurrentUpdate(t) {
			t.Errorf("testConcurrentUpdate failed")
			return
		}
		t.Logf("testConcurrentUpdate round %d succeeded", i)
	}
}

func testConcurrentUpdate(t *testing.T) (b bool) {
	assert := types.NewAssertion(t)
	cli := mustNewClient("localhost:6379", "")
	const key = "key_update"

	if !assert.NoError(cli.Put(context.Background(), key, types.NewValue([]byte("111"), 1))) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := cli.Set(context.Background(), key, types.NewValue(nil, 1).WithNoWriteIntent(), types.NewKVWriteOption().WithClearWriteIntent()); !assert.NoError(err) {
				return
			}
		}()
	}

	wg.Wait()
	val, err := cli.Get(context.Background(), key, types.NewKVReadOption(1).WithExactVersion())
	if !assert.NoError(err) || !assert.False(val.HasWriteIntent()) {
		return
	}
	return true
}

func TestConcurrentRemoveIf(t *testing.T) {
	for i := 0; i < rounds; i++ {
		if !testConcurrentRemoveIf(t) {
			t.Errorf("testConcurrentRemoveIf failed")
			return
		}
		t.Logf("testConcurrentRemoveIf round %d succeeded", i)
	}
}

func testConcurrentRemoveIf(t *testing.T) (b bool) {
	assert := types.NewAssertion(t)
	cli := mustNewClient("localhost:6379", "")
	const key = "key"

	if !assert.NoError(cli.Put(context.Background(), key, types.NewValue([]byte("111"), 1))) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < 10000; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := cli.Set(context.Background(), key, types.NewValue(nil, 1).WithNoWriteIntent(), types.NewKVWriteOption().WithRemoveVersion()); !assert.NoError(err) {
				return
			}
		}()
	}

	wg.Wait()
	if _, err := cli.Get(context.Background(), key, types.NewKVReadOption(1).WithExactVersion()); !errors.AssertIsVersionNotExistsErr(assert, err) {
		return
	}
	return true
}

// Actually this should never happen in real case.
func TestConcurrentUpdateRemoveIf(t *testing.T) {
	for i := 0; i < 1; i++ {
		if !testConcurrentUpdateRemoveIf(t) {
			t.Errorf("testConcurrentUpdateRemoveIf failed")
			return
		}
		t.Logf("testConcurrentUpdateRemoveIf round %d succeeded", i)
	}
}

func testConcurrentUpdateRemoveIf(t *testing.T) (b bool) {
	assert := types.NewAssertion(t)
	kv.Testing = true
	cli := mustNewClient("localhost:6379", "")
	const key = "key"

	if !assert.NoError(cli.Set(context.Background(), key, types.NewValue([]byte("111"), 1), types.NewKVWriteOption())) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < 10000; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			if i%2 == 0 {
				if err := cli.Set(context.Background(), key, types.NewValue(nil, 1).WithNoWriteIntent(), types.NewKVWriteOption().WithClearWriteIntent()); !errors.AssertNilOrErr(assert, err, errors.ErrVersionNotExists) {
					return
				}
			} else {
				if err := cli.Set(context.Background(), key, types.NewValue(nil, 1).WithNoWriteIntent(), types.NewKVWriteOption().WithRemoveVersion()); !errors.AssertNilOrErr(assert, err, errors.ErrCantRemoveCommittedValue) {
					return
				}
			}
		}(i)
	}

	wg.Wait()
	val, err := cli.Get(context.Background(), key, types.NewKVReadOption(1).WithExactVersion())
	if err == nil {
		return assert.Equal(uint64(1), val.Version) && assert.False(val.HasWriteIntent())
	}
	return errors.AssertIsErr(assert, err, errors.ErrVersionNotExists)
}
