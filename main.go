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
	roleFlag := flag.String("role", "follower", "the role of the node")
	config := flag.String("config", "config.json", "path to config file")
	id := flag.String("id", "", "id of the node")

	flag.Parse()

	nodes, err := loadConfig(*config)
	if err != nil {
		log.Fatalf("error loading config: %v", err)
	}

	_, ok := nodes[*id]
	if !ok {
		log.Fatalf("node %s not found", *id)
	}

	peers := []string{}

	for selfId, addr := range nodes {
		if selfId == *id {
			continue
		}
		peers = append(peers, addr)
	}

	if *roleFlag == "leader" {
		role = leader
		for _, addr := range peers {
			go func(addr string) {
				ticker := time.NewTicker(time.Millisecond * 200)
				for range ticker.C {
					con, err := net.Dial("tcp", addr)
					if err != nil {
						log.Printf("error connecting to %s: %v", addr, err)
						continue
					}
					reader := bufio.NewReader(con)
					_, err = con.Write([]byte("HEARTBEAT\n"))
					if err != nil {
						log.Printf("error writing to %s: %v", addr, err)
					}
					_, err = reader.ReadString('\n')
					if err != nil {
						log.Printf("error reading from %s: %v", addr, err)
					}
					con.Close()
				}
			}(addr)
		}
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
		case "UPDATETIME":
			result = fmt.Sprintf("%v", ViewUpdateTime())
		case "EXIT":
			con.Write([]byte("Connection closed\n"))
			return
		default:
			result = "Invalid Command"
		}
		con.Write([]byte(result + "\n"))
	}
}
