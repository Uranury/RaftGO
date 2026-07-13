package main

import (
	"bufio"
	"context"
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
	id              string
	mu              sync.Mutex
	role            string
	lastHeartbeat   time.Time
	electionTimeout time.Duration
	term            int
	votedFor        string
	votes           int
	sharedVar       int
	addr            string
	leaderAddr      string
	peers           []string
	stopLeader      context.CancelFunc
}

func main() {
	port := flag.String("port", ":9000", "port to listen on")
	roleFlag := flag.String("role", "follower", "the role of the node")
	config := flag.String("config", "config.json", "path to config file")
	id := flag.String("id", "", "id of the node")

	flag.Parse()

	root, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes, err := loadConfig(*config)
	if err != nil {
		log.Fatalf("error loading config: %v", err)
	}

	curAddr, ok := nodes[*id]
	if !ok {
		log.Fatalf("node %s not found", *id)
	}

	curNode := &Node{
		id:            *id,
		mu:            sync.Mutex{},
		role:          *roleFlag,
		addr:          curAddr,
		lastHeartbeat: time.Now(),
		sharedVar:     0,
		peers:         make([]string, 0),
		stopLeader:    func() {},
	}
	curNode.mu.Lock()
	curNode.resetElectionTimeout(500*time.Millisecond, 800*time.Millisecond, time.Now())
	curNode.mu.Unlock()

	for selfId, addr := range nodes {
		if selfId == curNode.id {
			continue
		}
		curNode.peers = append(curNode.peers, addr)
	}

	listener, err := net.Listen("tcp", *port)
	if err != nil {
		log.Fatalf("failed to start listening: %v", err)
	}

	go func() {
		ticker := time.NewTicker(time.Millisecond * 200)
		defer ticker.Stop()
		for range ticker.C {
			curNode.mu.Lock()
			elapsed := time.Since(curNode.lastHeartbeat)
			timeout := curNode.electionTimeout
			role := curNode.role

			if role != leader && elapsed > timeout {
				curNode.resetElectionTimeout(500*time.Millisecond, 800*time.Millisecond, time.Now())
				currentTerm := curNode.beginElection()
				curNode.mu.Unlock()

				curNode.broadcastVoteRequests(root, currentTerm)
			} else {
				curNode.mu.Unlock()
			}
		}
	}()

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
			node.mu.Lock()
			if node.role != leader {
				result = fmt.Sprintf("Current leader has addr: %s", node.leaderAddr)
			} else {
				node.Increment()
				node.replicateIncrement()
				result = "OK"
			}
			node.mu.Unlock()
		case command == "VALUE":
			node.mu.Lock()
			result = fmt.Sprintf("%d", node.GetValue())
			node.mu.Unlock()
		case command == "STATUS":
			node.mu.Lock()
			result = node.Status()
			node.mu.Unlock()
		case command == "HEALTH":
			result = fmt.Sprintf("%d", node.Health())
		case strings.HasPrefix(command, "HEARTBEAT"):
			var term int
			var leaderAddr string
			now := time.Now()
			fmt.Sscanf(command, "HEARTBEAT term=%d leader=%s", &term, &leaderAddr)
			node.mu.Lock()
			if node.term > term {
				result = fmt.Sprintf("Heartbeat sender has a lower term of: %d, own term: %d", term, node.term)
				node.mu.Unlock()
			} else if term > node.term {
				node.stepDown(term)
				node.resetElectionTimeout(500*time.Millisecond, 800*time.Millisecond, now)
				node.SetLeader(leaderAddr)
				node.mu.Unlock()
				result = fmt.Sprintf("New leader spotted with addr: %s", leaderAddr)
			} else {
				node.resetElectionTimeout(500*time.Millisecond, 800*time.Millisecond, now)
				node.SetLeader(leaderAddr)
				node.mu.Unlock()
				result = fmt.Sprintf("New heartbeat received at: %v", now)
			}
		case command == "UPDATETIME":
			node.mu.Lock()
			result = fmt.Sprintf("%v", node.ViewUpdateTime())
			node.mu.Unlock()
		case strings.HasPrefix(command, "REQUEST_VOTE"):
			var term int
			var candidate string
			fmt.Sscanf(command, "REQUEST_VOTE term=%d candidate=%s", &term, &candidate)

			node.mu.Lock()
			grant := false
			if term > node.term {
				node.stepDown(term)
			}
			if term == node.term && (node.votedFor == "" || node.votedFor == candidate) {
				node.resetElectionTimeout(500*time.Millisecond, 800*time.Millisecond, time.Now())
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
