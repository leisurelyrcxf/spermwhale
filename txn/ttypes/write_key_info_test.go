package ttypes

import (
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestEmptyWriteKeyInfo(t *testing.T) {
	testifyassert.False(t, EmptyWriteKeyInfo.NotEmpty())
	testifyassert.True(t, NewWriteKeyInfo(1).NotEmpty())
	testifyassert.True(t, NewWriteKeyInfo(255).NotEmpty())
}
