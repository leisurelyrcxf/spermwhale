package types

import (
	"math"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestSafeIncr(t *testing.T) {
	assert := testifyassert.New(t)

	u := MaxTxnVersion
	assert.Equal(uint64(math.MaxUint64), u)
	SafeIncr(&u)
	assert.Equal(uint64(math.MaxUint64), u)
	u += 1
	assert.Equal(uint64(0), u)
}

func TestMaxTxnVersion(t *testing.T) {
	date, err := time.Parse(time.RFC3339, MaxTxnVersionDate)
	if !testifyassert.NoError(t, err) {
		return
	}
	testifyassert.Equal(t, MaxTxnVersion, uint64(date.UnixNano()))
	testifyassert.Equal(t, uint64(0), MaxTxnVersion&((1<<12)-1))
	t.Logf("max txn version date: %s(%d)", date.String(), date.UnixNano())
}
