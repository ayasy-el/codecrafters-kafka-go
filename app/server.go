package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"time"
)

type Request struct {
	MessageSize   uint32
	ApiKey        uint16
	ApiVersion    uint16
	CorrelationId uint32
}

type Response struct {
	MessageSize   uint32
	CorrelationId uint32
	ErrorCode     uint16
	NumApiKeys    uint8
	ApiKey        uint16
	MinVersion    uint16
	MaxVersion    uint16
	TagByte       byte
	ThrottleTime  uint32
	TagByte2      byte
}

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092:", err)
		os.Exit(1)
	}
	fmt.Println("Listening on port 9092")

	for {
		if conn, err := listener.Accept(); err == nil {
			go handleConnection(conn)
		} else {
			fmt.Println("Error accepting connection:", err)
		}
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))

		var req Request
		if err := binary.Read(conn, binary.BigEndian, &req); err != nil {
			fmt.Println("Error reading request:", err)
			return
		}
		conn.Read(make([]byte, 1024))

		resp := Response{
			MessageSize:   19,
			CorrelationId: req.CorrelationId,
			ErrorCode:     0,
			NumApiKeys:    2,
			ApiKey:        req.ApiKey,
			MinVersion:    0,
			MaxVersion:    4,
			TagByte:       0,
			ThrottleTime:  0,
			TagByte2:      0,
		}

		if req.ApiVersion > 4 {
			resp.ErrorCode = 35
		}

		if err := binary.Write(conn, binary.BigEndian, &resp); err != nil {
			fmt.Println("Error sending response:", err)
		}
	}
}
