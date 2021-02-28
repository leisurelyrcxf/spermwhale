package topo

import (
	"encoding/json"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/leisurelyrcxf/spermwhale/utils"
)

type Cluster struct {
	GroupNumber int `json:"group_number"`
}

type Group struct {
	Id         int    `json:"id"`
	ServerAddr string `json:"server_addr"`
}

func (g *Group) Encode() []byte {
	return utils.JsonEncode(g)
}

func jsonDecode(v interface{}, b []byte) error {
	if err := json.Unmarshal(b, v); err != nil {
		return errors.Trace(err)
	}
	return nil
}
