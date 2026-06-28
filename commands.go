package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
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
