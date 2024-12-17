package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
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

	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Error reading data:", err)
		return
	}

	var req Request
	if err := binary.Read(bytes.NewReader(buffer[:n]), binary.BigEndian, &req); err != nil {
		fmt.Println("Error reading request:", err)
		return
	}

	resp := Response{CorrelationId: req.CorrelationId}
	if err := binary.Write(conn, binary.BigEndian, &resp); err != nil {
		fmt.Println("Error sending response:", err)
	}
}
