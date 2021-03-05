package memory

import (
	"testing"

	"github.com/leisurelyrcxf/spermwhale/kv"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

func TestMemoryDB(t *testing.T) {
	cli := NewMemoryDB()
	for i := 0; i < 1000; i++ {
		for _, dbug := range []bool{true, false} {
			utils.SetCustomizedDebugFlag(dbug)
			if !kv.TestDB(t, cli) {
				t.Errorf("TestMemoryDB failed @round %d, debug: %v", i, dbug)
				return
			}
			t.Logf("TestMemoryDB succeeded @round %d, debug: %v", i, dbug)
		}
	}
}
