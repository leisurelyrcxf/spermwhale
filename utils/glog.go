package utils

import (
	"flag"
	"strconv"
)

func SetLogLevel(v int) func() {
	gFlag := flag.Lookup("v")
	old := getLogLevel(gFlag)
	setLogLevel(gFlag, v)
	return func() {
		setLogLevel(gFlag, old)
	}
}

func setLogLevel(gFlag *flag.Flag, v int) {
	if err := gFlag.Value.Set(strconv.Itoa(v)); err != nil {
		panic(err)
	}
}

func WithLogLevel(v int, f func()) {
	defer SetLogLevel(v)()
	f()
}

func GetLogLevel() int {
	return getLogLevel(flag.Lookup("v"))
}

func getLogLevel(gFlag *flag.Flag) int {
	if gFlag == nil {
		panic("flag 'v' is nil")
	}
	s := gFlag.Value.String()
	i, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return i
}
