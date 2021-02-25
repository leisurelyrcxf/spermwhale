package models

import (
	"encoding/json"

	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/utils/errors"
)

type Cluster struct {
	GroupNumber int `json:"group_number"`
}

type Group struct {
	Id         int    `json:"id"`
	ServerAddr string `json:"server_addr"`
}

func (g *Group) Encode() []byte {
	return jsonEncode(g)
}

func jsonEncode(v interface{}) []byte {
	b, err := json.MarshalIndent(v, "", "    ")
	if err != nil {
		glog.Fatalf("encode to json failed: '%v'", err)
	}
	return b
}

func jsonDecode(v interface{}, b []byte) error {
	if err := json.Unmarshal(b, v); err != nil {
		return errors.Trace(err)
	}
	return nil
}
