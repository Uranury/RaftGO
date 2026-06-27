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

func ViewUpdateTime(node *Node) time.Time {
	return node.lastHeartbeat
}

func UpdateLastHeartbeat(node *Node, t time.Time) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.lastHeartbeat = t
}

func GetValue(node *Node) int {
	node.mu.Lock()
	defer node.mu.Unlock()
	return node.sharedVar
}

func Increment(node *Node) {
	node.mu.Lock()
	defer node.mu.Unlock()
	prev := node.sharedVar
	node.sharedVar++
	log.Printf("The previous value was %d, updated value is: %d", prev, node.sharedVar)
}

func Status(node *Node) int {
	node.mu.Lock()
	defer node.mu.Unlock()
	return node.role
}

func Health(_ *Node) int {
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
