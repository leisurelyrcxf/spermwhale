package topo

import (
	"github.com/leisurelyrcxf/spermwhale/utils"
)

type Group struct {
	Id         int    `json:"id"`
	ServerAddr string `json:"server_addr"`
}

func (g *Group) Encode() []byte {
	return utils.JsonEncode(g)
}
