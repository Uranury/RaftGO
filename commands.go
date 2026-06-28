package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

// helper
func loadConfig(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	nodes := make(map[string]string)
	if err = json.Unmarshal(data, &nodes); err != nil {
		return nil, fmt.Errorf("failed to parse %q: %v", path, err)
	}
	return nodes, nil
}

func (n *Node) ViewUpdateTime() time.Time {
	return n.lastHeartbeat
}

func (n *Node) UpdateLastHeartbeat(t time.Time) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.lastHeartbeat = t
}

func (n *Node) GetValue() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.sharedVar
}

func (n *Node) Increment() {
	n.mu.Lock()
	defer n.mu.Unlock()
	prev := n.sharedVar
	n.sharedVar++
	log.Printf("The previous value was %d, updated value is: %d", prev, n.sharedVar)
}

func (n *Node) Status() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

func (n *Node) Health() int {
	return 1
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.term++
	n.role = candidate
	n.votedFor = n.id
	n.votes = 1
	currentTerm := n.term
	n.mu.Unlock()

	for _, addr := range n.peers {
		go func(addr string) {
			granted, err := n.requestVote(addr, currentTerm)
			if err != nil {
				log.Printf("Failed to vote to %s : %v", addr, err)
				return
			}
			if !granted {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.role != candidate || n.term != currentTerm {
				return
			}
			n.votes++
			if n.votes*2 > len(n.peers)+1 {
				n.role = leader
				log.Printf("won election for term %d", currentTerm)
			}
		}(addr)
	}
}

func (n *Node) requestVote(addr string, term int) (bool, error) {
	con, err := net.Dial("tcp", addr)
	if err != nil {
		return false, err
	}
	defer con.Close()

	msg := fmt.Sprintf("REQUEST_VOTE term=%d candidate=%s\n", term, n.id)
	_, err = con.Write([]byte(msg))
	if err != nil {
		return false, err
	}

	reader := bufio.NewReader(con)
	line, err := reader.ReadString('\n')
	if err != nil {
		return false, err
	}

	res := strings.TrimSpace(line)
	return res == "GRANTED", nil
}

/*
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
*/
