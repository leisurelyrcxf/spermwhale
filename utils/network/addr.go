package network

import "net"

func GetLocalAddr(dest string) (string, error) {
	conn, err := net.Dial("tcp", dest)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.TCPAddr)
	return localAddr.String(), nil
}
