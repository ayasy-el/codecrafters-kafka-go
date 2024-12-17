package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

type Response struct {
	MessageSize   uint32 // 4 byte
	CorrelationId uint32 // 4 byte
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	fmt.Println("Listening on port 9092")

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	var response Response
	response.CorrelationId = 7

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, response); err != nil {
		fmt.Println("Error writing response:", err)
		return
	}

	if _, err := conn.Write(buf.Bytes()); err != nil {
		fmt.Println("Error sending response to client:", err)
		return
	}
}
