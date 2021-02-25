package utils

import (
	"encoding/json"

	"github.com/golang/glog"
)

func JsonEncode(v interface{}) []byte {
	b, err := json.MarshalIndent(v, "", "    ")
	if err != nil {
		glog.Fatalf("encode to json failed: '%v'", err)
	}
	return b
}
