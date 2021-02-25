package integration_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	integration "github.com/leisurelyrcxf/spermwhale/integration_test"
)

func TestTransaction(t *testing.T) {
	ts := integration.NewTestSuite(t)
	if !assert.NotNil(t, ts) {
		return
	}

}
