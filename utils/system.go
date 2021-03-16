package utils

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/golang/glog"
)

const MaxPort = 65535

func IsPortAvailable(port int) bool {
	return nil == TryPort(port)
}

func TryPort(port int) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	if err := ln.Close(); err != nil {
		panic(err)
	}
	return nil
}

func FindAvailablePort(port int, isPortAvailable func(int) bool) int {
	for i := port; i < MaxPort+1; i++ {
		if isPortAvailable(i) {
			return i
		}
	}
	return MaxPort + 1
}

func PrepareEmptyDir(dir string) error {
	if err := RemoveDirIfExists(dir); err != nil {
		return err
	}
	return MkdirIfNotExists(dir)
}

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

func MkdirIfNotExists(dir string) error {
	if DirExists(dir) {
		return nil
	}
	err := os.MkdirAll(dir, 0700)
	if err != nil {
		glog.Errorf("[MkdirIfNotExists] can't create directory '%s', err: '%v'", dir, err)
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

func CatFile(filePath string, desc string) {
	f, err := os.Open(filePath)
	if err != nil {
		glog.Errorf("[catFile] open %s file '%s' failed, err: '%v'", desc, filePath, err)
		return
	}
	defer f.Close()

	glog.Warningf("[catFile] print %s file(%s)'s content", desc, filePath)
	br := bufio.NewReader(f)
	for {
		l, _, err := br.ReadLine()
		if err != nil {
			break
		}
		fmt.Println(string(l))
	}
}

func Now() *time.Time {
	now := time.Now()
	return &now
}

func IsKilled(err error) bool {
	return isSigError(err, os.Kill)
}

func IsTerminated(err error) bool {
	return isSigError(err, os.Kill)
}

func isSigError(err error, sig os.Signal) bool {
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		return false
	}
	ps := exitErr.ProcessState
	if ps == nil {
		return false
	}
	status := ps.Sys().(syscall.WaitStatus)
	switch {
	case status.Signaled():
		return status.Signal() == sig
	case status.Stopped():
		return status.StopSignal() == sig
	//case status.Exited():
	//	return false
	//case status.Continued():
	//	return false
	default:
		return false
	}
}

func GetCoordinatorName(enableV3 bool) string {
	if enableV3 {
		return "etcdv3"
	}
	return "etcd"
}

func DeepCopyList(in []string) (out []string) {
	out = append(out, in...)
	return out
}

func ConvertToRelativePath(path string) string {
	if filepath.IsAbs(path) {
		return path[1:]
	}
	return path
}

func Bool2Int(b bool) int {
	if b {
		return 1
	}
	return 0
}

func NonInPlaceSort(array []string) []string {
	copied := copyStringArray(array)
	sort.Strings(copied)
	return copied
}

func GenerateAllPossibleOrders(array []string) [][]string {
	var result [][]string
	generateAllPossibleOrders(array, array, &result)
	return result
}

func generateAllPossibleOrders(invarArray []string, varArray []string, result *[][]string) {
	if len(varArray) == 0 {
		*result = append(*result, copyStringArray(invarArray))
	}
	for i := 0; i < len(varArray); i++ {
		swap(varArray, 0, i)
		generateAllPossibleOrders(invarArray, varArray[1:], result)
		swap(varArray, 0, i)
	}
}

func swap(array []string, i, j int) {
	array[j], array[i] = array[i], array[j]
}

func copyStringArray(array []string) (ret []string) {
	ret = append(ret, array...)
	return
}

func BinaryExistsInPath(processName string) bool {
	path, err := exec.LookPath(processName)
	if err != nil {
		glog.Errorf("[BinaryExistsInPath] can't find binary %s in path", processName)
		return false
	}
	glog.Infof("[BinaryExistsInPath] found binary %s in path", path)
	return true
}

func NewBool(b bool) *bool {
	pb := new(bool)
	*pb = b
	return pb
}
