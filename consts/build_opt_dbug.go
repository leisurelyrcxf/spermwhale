// +build DBUG

package consts

const BuildOption = buildOptDebug

func init() {
    if !BuildOption.IsDebug() {
        panic("!BuildOption.IsDebug()")
    }
}