package testutils

import (
	"testing"

	"github.com/leisurelyrcxf/spermwhale/utils"

	"github.com/leisurelyrcxf/spermwhale/types"
)

func RunTestForNRounds(t *testing.T, rounds int, testCase func(t types.T) (b bool)) {
	for i := 0; i < rounds; i++ {
		if !testCase(t) {
			t.Errorf("%s failed @round %d", t.Name(), i)
			return
		}
		if i%(utils.MaxInt(1, rounds/100)) == 0 {
			t.Logf("%s succeeded %d rounds", t.Name(), i)
		}
	}
}
