package utils

import (
	"encoding/json"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/golang/glog"
)

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
