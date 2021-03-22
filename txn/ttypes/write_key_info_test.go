package ttypes

import (
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestEmptyWriteKeyInfo(t *testing.T) {
	testifyassert.True(t, EmptyWriteKeyInfo.IsEmpty())
	testifyassert.False(t, NewWriteKeyInfo(1).IsEmpty())
	testifyassert.False(t, NewWriteKeyInfo(255).IsEmpty())
}
