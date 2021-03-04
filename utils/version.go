package utils

import (
	"fmt"
	"runtime"

	"github.com/leisurelyrcxf/spermwhale/build_opt"
	"github.com/leisurelyrcxf/spermwhale/versioninfo"
)

// VersionString show version thing
func VersionString(ver, rev, buildAt string) string {
	version := ""
	if build_opt.Debug {
		version += fmt.Sprintf("Debug\n")
	} else {
		version += fmt.Sprintf("Release\n")
	}
	version += fmt.Sprintf("Version:        %s\n", ver)
	version += fmt.Sprintf("Git hash:       %s\n", rev)
	version += fmt.Sprintf("Built:          %s\n", buildAt)
	version += fmt.Sprintf("Golang version: %s\n", runtime.Version())
	version += fmt.Sprintf("OS/Arch:        %s/%s\n", runtime.GOOS, runtime.GOARCH)
	return version
}

// Version shows version thing
func Version() string {
	return VersionString(
		versioninfo.VERSION,
		versioninfo.REVISION,
		versioninfo.BUILTAT,
	)
}
