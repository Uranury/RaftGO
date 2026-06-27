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

type Node struct {
	id            string
	mu            sync.Mutex
	role          int
	lastHeartbeat time.Time
	sharedVar     int
	peers         []string
}

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

	curNode := &Node{
		id:            *id,
		mu:            sync.Mutex{},
		role:          follower,
		lastHeartbeat: time.Now(),
		sharedVar:     0,
		peers:         make([]string, 0),
	}

	for selfId, addr := range nodes {
		if selfId == curNode.id {
			continue
		}
		curNode.peers = append(curNode.peers, addr)
	}

	if *roleFlag == "leader" {
		curNode.role = leader
		for _, addr := range curNode.peers {
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
		go handleConnection(con, curNode)
	}
}

func handleConnection(con net.Conn, node *Node) {
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
			node.Increment()
			result = "OK"
		case "VALUE":
			result = fmt.Sprintf("%d", node.GetValue())
		case "STATUS":
			result = fmt.Sprintf("%d", node.Status())
		case "HEALTH":
			result = fmt.Sprintf("%d", node.Health())
		case "HEARTBEAT":
			now := time.Now()
			node.UpdateLastHeartbeat(now)
			result = fmt.Sprintf("New heartbeat received at: %v", now)
		case "UPDATETIME":
			result = fmt.Sprintf("%v", node.ViewUpdateTime())
		case "EXIT":
			con.Write([]byte("Connection closed\n"))
			return
		default:
			result = "Invalid Command"
		}
		con.Write([]byte(result + "\n"))
	}
}
