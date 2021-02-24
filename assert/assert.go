// Licensed under the MIT (MIT-LICENSE.txt) license.

package assert

import (
	"fmt"
)

func Must(b bool) {
	if b {
		return
	}
	panic("assertion failed")
}

func MustNoError(err error) {
	if err == nil {
		return
	}
	panic(fmt.Sprintf("'%s', error happens, assertion failed", err.Error()))
}
