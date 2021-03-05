package utils

import "github.com/leisurelyrcxf/spermwhale/consts"

var customizedDebugFlag *bool

func SetCustomizedDebugFlag(b bool) {
	customizedDebugFlag = new(bool)
	*customizedDebugFlag = b
}

func IsDebug() bool {
	if customizedDebugFlag != nil {
		return *customizedDebugFlag
	}
	return consts.BuildOption.IsDebug()
}
