package network

import "net"

func GetLocalIP(dest string) (string, error) {
	conn, err := net.Dial("tcp", dest)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.TCPAddr)
	return localAddr.IP.String(), nil
}
