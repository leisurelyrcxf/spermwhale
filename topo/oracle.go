package topo

import (
	"github.com/leisurelyrcxf/spermwhale/utils"
)

type Oracle struct {
	ServerAddr string `json:"server_addr"`
}

func (o *Oracle) Encode() []byte {
	return utils.JsonEncode(o)
}
