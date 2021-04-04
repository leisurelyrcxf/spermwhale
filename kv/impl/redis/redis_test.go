package redis

import (
	"testing"

	"github.com/leisurelyrcxf/spermwhale/types"

	"github.com/leisurelyrcxf/spermwhale/kv"
)

const rounds = 1000

func newRedis() (*types.DB, error) {
	return newDB("127.0.0.1:6379", "", 0)
}

func TestDB(t *testing.T) {
	kv.RunTestCase(t, rounds, newRedis, kv.TestDB)
}

func TestCausalConsistency(t *testing.T) {
	kv.RunTestCase(t, 100000, newRedis, kv.TestCausalConsistency)
}

func TestConcurrentClearWriteIntent(t *testing.T) {
	kv.RunTestCase(t, rounds, newRedis, kv.TestConcurrentClearWriteIntent)
}

func TestConcurrentRemoveVersion(t *testing.T) {
	kv.RunTestCase(t, rounds, newRedis, kv.TestConcurrentRemoveVersion)
}

func TestConcurrentClearWriteIntentRemoveVersion(t *testing.T) {
	kv.RunTestCase(t, rounds, newRedis, kv.TestConcurrentClearWriteIntentRemoveVersion)
}
