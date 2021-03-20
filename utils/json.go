package utils

import (
	"encoding/json"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/errors"
)

func JsonEncodeCompacted(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		glog.Fatalf("encode to json failed: '%v'", err)
	}
	return b
}

func JsonEncode(v interface{}) []byte {
	b, err := json.MarshalIndent(v, "", "    ")
	if err != nil {
		glog.Fatalf("encode to json failed: '%v'", err)
	}
	return b
}

func JsonDecode(v interface{}, b []byte) error {
	if err := json.Unmarshal(b, v); err != nil {
		return errors.Trace(err)
	}
	return nil
}
