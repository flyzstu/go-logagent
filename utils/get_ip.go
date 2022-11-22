package utils

import (
	"net"
	"strings"
)

func GetOutboundIP() (ip string, err error) {
	conn, err := net.Dial("udp", "1.1.1.1:80")
	if err != nil {
		return
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	ip = strings.Split(localAddr.String(), ":")[0]
	return
}
