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
	leader    = "leader"
	follower  = "follower"
	candidate = "candidate"
)

type Node struct {
	id            string
	mu            sync.Mutex
	role          string
	lastHeartbeat time.Time
	term          int
	votedFor      string
	votes         int
	sharedVar     int
	peers         []string
	stopLeader    chan struct{}
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
		role:          *roleFlag,
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

	go func() {
		ticker := time.NewTicker(time.Millisecond * 200)
		defer ticker.Stop()
		for range ticker.C {
			curNode.mu.Lock()
			elapsed := time.Since(curNode.lastHeartbeat)
			role := curNode.role
			curNode.mu.Unlock()

			if role != leader && elapsed > time.Millisecond*500 {
				curNode.startElection()
			}
		}
	}()

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

		switch {
		case command == "INCREMENT":
			node.Increment()
			result = "OK"
		case command == "VALUE":
			result = fmt.Sprintf("%d", node.GetValue())
		case command == "STATUS":
			result = fmt.Sprintf("%s", node.Status())
		case command == "HEALTH":
			result = fmt.Sprintf("%d", node.Health())
		case command == "HEARTBEAT":
			now := time.Now()
			node.UpdateLastHeartbeat(now)
			result = fmt.Sprintf("New heartbeat received at: %v", now)
		case command == "UPDATETIME":
			result = fmt.Sprintf("%v", node.ViewUpdateTime())
		case strings.HasPrefix(command, "REQUEST_VOTE"):
			var term int
			var candidate string
			fmt.Sscanf(command, "REQUEST_VOTE term=%d candidate=%s", &term, &candidate)

			node.mu.Lock()
			grant := false
			if term > node.term {
				node.term = term
				node.votedFor = ""
				node.role = follower
			}
			if term == node.term && (node.votedFor == "" || node.votedFor == candidate) {
				node.votedFor = candidate
				node.lastHeartbeat = time.Now()
				grant = true
			}
			node.mu.Unlock()
			if grant {
				result = "GRANTED"
			} else {
				result = "DENIED"
			}
		case command == "EXIT":
			con.Write([]byte("Connection closed\n"))
			return
		default:
			result = "Invalid Command"
		}
		con.Write([]byte(result + "\n"))
	}
}
