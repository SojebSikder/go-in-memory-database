package internal

import (
	"flag"
	"fmt"
	"net"
	"strings"
)

var port = "6379"

func StartServer() {
	flag.StringVar(&port, "port", "6379", "port to listen on")
	flag.Parse()

	fmt.Println("Listening on port :" + port)

	// Create a new server
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer listener.Close()

	aof, err := NewAof("database.aof")
	if err != nil {
		fmt.Println("Error opening AOF:", err)
		return
	}
	defer aof.Close()

	// Load data from AOF
	aof.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command from AOF:", command)
			return
		}

		handler(args)
	})

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		go handleConnection(conn, aof)
	}
}

func handleConnection(conn net.Conn, aof *Aof) {
	defer conn.Close()

	resp := NewResp(conn)
	writer := NewWriter(conn)

	for {
		value, err := resp.Read()
		if err != nil {
			if err.Error() != "EOF" {
				fmt.Println("Error reading:", err)
			}
			return
		}

		if value.typ != "array" || len(value.array) == 0 {
			writer.Write(Value{typ: "string", str: "Invalid request"})
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			writer.Write(Value{typ: "string", str: "Unknown command"})
			continue
		}

		if command == "SET" || command == "HSET" || command == "DEL" || command == "FLUSHALL" ||
			command == "HDEL" || command == "INCR" || command == "DECR" {
			aof.Write(value)
		}

		result := handler(args)
		writer.Write(result)
	}
}
