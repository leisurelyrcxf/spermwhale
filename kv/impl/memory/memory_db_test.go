package memory

import (
	"testing"

	"github.com/leisurelyrcxf/spermwhale/kv"
)

const rounds = 10000

func newDB() (*kv.DB, error) {
	return NewMemoryDB(), nil
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
