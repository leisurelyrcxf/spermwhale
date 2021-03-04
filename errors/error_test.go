package errors

import (
	"testing"

	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/leisurelyrcxf/spermwhale/consts"
)

func TestError_Error(t *testing.T) {
	assert := testifyassert.New(t)

	assert.Equal(consts.ErrCodeUnknown, GetErrorCode(nil))
	var e = (*Error)(nil)
	assert.Equal(consts.ErrCodeUnknown, GetErrorCode(e))

	assert.Equal(consts.ErrCodeUnknown, GetErrorCode((*commonpb.Error)(nil)))
}
