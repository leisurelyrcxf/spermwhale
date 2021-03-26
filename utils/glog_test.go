package utils

import (
	"flag"
	"fmt"
	"testing"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/golang/glog"
)

func TestSetLogLevel(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", "10")

	glog.V(11).Infof("11_10")
	glog.V(10).Infof("10_10")
	glog.V(9).Infof("9_10")

	SetLogLevel(12)
	glog.V(11).Infof("11_12")
	glog.V(10).Infof("10_12")
	glog.V(9).Infof("9_12")

	SetLogLevel(7)
	glog.V(11).Infof("11_7")
	glog.V(10).Infof("10_7")
	glog.V(9).Infof("9_7")

	WithLogLevel(5, func() {
		testifyassert.Equal(t, 5, GetLogLevel())
	})
	testifyassert.Equal(t, 7, GetLogLevel())
}
