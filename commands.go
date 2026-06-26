package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

func ViewUpdateTime() time.Time {
	return lastHeartbeat
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
