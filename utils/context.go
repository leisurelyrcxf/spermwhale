package utils

import "context"

var CancelledContext, cancelledContextCancel = context.WithCancel(context.Background())

func init() {
	cancelledContextCancel()
}
