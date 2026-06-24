package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

const (
	leader = iota
	follower
	candidate
)

var (
	sharedVar int = 0
	role          = follower
	mu        sync.Mutex
)

func main() {
	listener, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("failed to start listening: %v", err)
	}
	for {
		con, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleConnection(con)
	}

}

func GetValue() int {
	defer mu.Unlock()
	mu.Lock()
	return sharedVar
}

func Increment() {
	defer mu.Unlock()
	mu.Lock()
	prev := sharedVar
	sharedVar++
	log.Printf("The previous value was %d, updated value is: %d", prev, sharedVar)
}

func Status() int {
	defer mu.Unlock()
	mu.Lock()
	return role
}

func Health() int {
	return 1
}

func healthCheck(host, port string) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.Write([]byte("HEALTH\n"))

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	result := strings.TrimSpace(line)
	if result != "1" {
		return fmt.Errorf("health check failed or timed out")
	}
	return nil
}

func handleConnection(con net.Conn) {
	defer con.Close()

	reader := bufio.NewReader(con)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("failed to read from a connection")
			return
		}
		var result string
		command := strings.TrimSpace(line)

		switch command {
		case "INCREMENT":
			Increment()
			result = "OK"
		case "VALUE":
			result = fmt.Sprintf("%d", GetValue())
		case "STATUS":
			result = fmt.Sprintf("%d", Status())
		case "HEALTH":
			result = fmt.Sprintf("%d", Health())
		case "EXIT":
			con.Write([]byte("Connection closed\n"))
			return
		default:
			result = "Invalid Command"
		}
		con.Write([]byte(result + "\n"))
	}
}
