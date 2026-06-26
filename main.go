package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	leader = iota
	follower
	candidate
)

var (
	sharedVar     int = 0
	role              = follower
	mu            sync.Mutex
	lastHeartbeat time.Time
)

func main() {
	port := flag.String("port", ":9000", "port to listen on")
	peer := flag.String("peer", "", "address of the peer node")
	roleFlag := flag.String("role", "follower", "the role of the node")

	flag.Parse()

	if *roleFlag == "leader" {
		go func() {
			ticker := time.NewTicker(time.Millisecond * 200)
			for range ticker.C {
				con, err := net.Dial("tcp", *peer)
				if err != nil {
					log.Printf("failed to reach follower: %v", err)
					continue
				}
				_, err = con.Write([]byte("HEARTBEAT\n"))
				if err != nil {
					log.Printf("failed to send heartbeat: %v", err)
				}
				con.Close()
			}
		}()
	}

	listener, err := net.Listen("tcp", *port)
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

func UpdateLastHeartbeat(t time.Time) {
	defer mu.Unlock()
	mu.Lock()
	lastHeartbeat = t
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
			if errors.Is(err, io.EOF) {
				return
			}
			log.Printf("failed to read from a connection: %v", err)
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
		case "HEARTBEAT":
			now := time.Now()
			UpdateLastHeartbeat(now)
			result = fmt.Sprintf("New heartbeat received at: %v", now)
		case "EXIT":
			con.Write([]byte("Connection closed\n"))
			return
		default:
			result = "Invalid Command"
		}
		con.Write([]byte(result + "\n"))
	}
}
