package transaction

import (
	"testing"

	"github.com/leisurelyrcxf/spermwhale/assert"
)

func TestInvalidWaiters(t *testing.T) {
	assert.Must(invalidKeyWaiters != nil)
	assert.Must(isInvalidKeyWaiters(invalidKeyWaiters))
	assert.Must(isInvalidKeyWaiters([]*KeyEventWaiter{}))
	assert.Must(!isInvalidKeyWaiters(nil))
}
