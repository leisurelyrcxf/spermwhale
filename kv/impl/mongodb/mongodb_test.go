package mongodb

import (
	"testing"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/leisurelyrcxf/spermwhale/kv"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

func TestMongo(t *testing.T) {
	cli, err := NewDB("localhost:27017", nil)
	if !testifyassert.NoError(t, err) {
		return
	}
	for i := 0; i < 1000; i++ {
		for _, dbug := range []bool{true, false} {
			utils.SetCustomizedDebugFlag(dbug)
			if !kv.TestDB(t, cli) {
				t.Errorf("testMongo failed @round %d, debug: %v", i, dbug)
				return
			}
			t.Logf("testRredis succeeded @round %d, debug: %v", i, dbug)
		}
	}
}
