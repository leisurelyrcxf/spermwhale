package mongodb

import (
	"testing"

	"github.com/leisurelyrcxf/spermwhale/types"

	"github.com/leisurelyrcxf/spermwhale/kv"
)

const rounds = 1000

func newDB() (*types.DB, error) {
	//return NewDB("localhost:37037", nil)
	return NewDB("localhost:27017", nil)
}

func TestDB(t *testing.T) {
	kv.RunTestCase(t, rounds, newDB, kv.TestDB)
}

func TestCausalConsistency(t *testing.T) {
	kv.RunTestCase(t, 100000, newDB, kv.TestCausalConsistency)
}

func TestConcurrentClearWriteIntent(t *testing.T) {
	kv.RunTestCase(t, rounds, newDB, kv.TestConcurrentClearWriteIntent)
}

func TestConcurrentRemoveVersion(t *testing.T) {
	kv.RunTestCase(t, rounds, newDB, kv.TestConcurrentRemoveVersion)
}

func TestConcurrentClearWriteIntentRemoveVersion(t *testing.T) {
	kv.RunTestCase(t, rounds, newDB, kv.TestConcurrentClearWriteIntentRemoveVersion)
}
