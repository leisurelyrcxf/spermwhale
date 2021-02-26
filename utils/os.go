package utils

import (
	"os"

	"github.com/golang/glog"
)

func RemoveDirIfExists(dir string) error {
	if !DirExists(dir) {
		return nil
	}
	err := os.RemoveAll(dir)
	if err != nil {
		glog.Errorf("[RemoveDirIfExists] can't create directory '%s', err: '%v'", dir, err)
	}
	return err
}

func DirExists(dir string) bool {
	info, err := os.Stat(dir)
	if err == nil {
		return info.IsDir()
	}
	return false
}
