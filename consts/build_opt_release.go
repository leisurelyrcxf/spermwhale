// +build !DBUG

package consts

const BuildOption = buildOptRelease

func init() {
    if BuildOption.IsDebug() {
        panic("BuildOption.IsDebug()")
    }
}