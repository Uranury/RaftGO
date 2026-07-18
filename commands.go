package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
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

func (n *Node) GetValue() int {
	return n.sharedVar
}

func (n *Node) Increment() {
	prev := n.sharedVar
	n.sharedVar++
	log.Printf("The previous value was %d, updated value is: %d", prev, n.sharedVar)
}

func (n *Node) Status() string {
	return n.role
}

func (n *Node) SetLeader(leaderAddr string) {
	n.leaderAddr = leaderAddr
}

func (n *Node) Health() int {
	return 1
}

func (n *Node) resetElectionTimeout(min, max time.Duration, t time.Time) {
	n.lastHeartbeat = t
	n.electionTimeout = min + time.Duration(rand.Int64N(int64(max-min)))
}

// beginElection transitions n into a candidate for a new term and returns
// that term. The caller must already hold n.mu: folding the eligibility
// check and this mutation into the same critical section as the caller's
// "should I run for election" decision prevents a concurrent vote grant or
// heartbeat (which would make that decision stale) from being clobbered.
func (n *Node) beginElection() int {
	n.term++
	n.role = candidate
	n.votedFor = n.id
	n.votes = 1
	return n.term
}

func (n *Node) broadcastVoteRequests(ctx context.Context, currentTerm int) {
	for _, addr := range n.peers {
		go func(addr string) {
			granted, err := n.requestVote(addr, currentTerm)
			if err != nil {
				log.Printf("Failed to request vote from %s: %v", addr, err)
				return
			}
			if !granted {
				return
			}
			n.mu.Lock()
			if n.role != candidate || n.term != currentTerm {
				n.mu.Unlock()
				return
			}
			n.votes++
			won := n.votes*2 > len(n.peers)+1
			firstWin := won && n.role != leader
			if won {
				n.role = leader
			}
			n.mu.Unlock()
			if firstWin {
				log.Printf("won election for term %d", currentTerm)
				heartbeatCtx, cancel := context.WithCancel(ctx)
				n.startHeartbeats(heartbeatCtx, cancel)
			}
		}(addr)
	}
}

func (n *Node) startHeartbeats(stop context.Context, cancel context.CancelFunc) {
	n.mu.Lock()
	n.stopLeader = cancel
	peers := append([]string(nil), n.peers...)
	n.mu.Unlock()

	for _, addr := range peers {
		go func(addr string) {
			ticker := time.NewTicker(time.Millisecond * 200)
			defer ticker.Stop()
			for {
				select {
				case <-stop.Done():
					return
				case <-ticker.C:
					conn, err := net.DialTimeout("tcp", addr, time.Millisecond*200)
					if err != nil {
						continue
					}
					n.mu.Lock()
					msg := fmt.Sprintf("HEARTBEAT term=%d leader=%s\n", n.term, n.addr)
					n.mu.Unlock()
					conn.Write([]byte(msg))
					bufio.NewReader(conn).ReadString('\n')
					conn.Close()
				}
			}
		}(addr)
	}
}

func (n *Node) requestVote(addr string, term int) (bool, error) {
	con, err := net.DialTimeout("tcp", addr, time.Millisecond*200)
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

func (n *Node) stepDown(newTerm int) {
	n.term = newTerm
	n.role = follower
	n.votedFor = ""
	n.stopLeader()
}

func (n *Node) replicateIncrement() {
	n.mu.Lock()
	peers := append([]string(nil), n.peers...)
	n.mu.Unlock()
	for _, addr := range peers {
		go func(addr string) {
			con, err := net.DialTimeout("tcp", addr, time.Millisecond*200)
			if err != nil {
				return
			}
			defer con.Close()
			_, err = con.Write([]byte("REPLICATE_INCREMENT\n"))
			if err != nil {
				return
			}
			reader := bufio.NewReader(con)
			_, err = reader.ReadString('\n')
			if err != nil {
				return
			}
		}(addr)
	}
}
