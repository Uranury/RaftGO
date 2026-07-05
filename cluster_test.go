package main

// Integration tests that drive a real cluster of raftgo processes over TCP,
// the same way the manual repro scripts did. They exist so a leader-election
// regression (split brain, redundant elections, id/address misconfiguration)
// gets caught by `go test` instead of by hand-rolled shell scripts.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var testBinPath string

func TestMain(m *testing.M) {
	tmpDir, err := os.MkdirTemp("", "raftgo-bin")
	if err != nil {
		fmt.Println("failed to create temp dir for test binary:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)

	testBinPath = filepath.Join(tmpDir, "raftgo_test_bin")
	wd, err := os.Getwd()
	if err != nil {
		fmt.Println("failed to get working directory:", err)
		os.Exit(1)
	}

	cmd := exec.Command("go", "build", "-o", testBinPath, ".")
	cmd.Dir = wd
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("failed to build raftgo binary: %v\n%s\n", err, out)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

// freePort asks the OS for an unused TCP port on localhost.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func writeConfig(t *testing.T, dir string, nodes map[string]string) string {
	t.Helper()
	data, err := json.Marshal(nodes)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}
	return path
}

// testNode is one running raftgo process under test.
type testNode struct {
	addr    string
	logPath string
	cmd     *exec.Cmd
}

// startNode launches a raftgo process bound to addr's port, registering with
// the cluster under asIfID. Passing an asIfID that doesn't match the id
// whose config entry equals addr simulates an operator swapping node ids
// between machines.
func startNode(t *testing.T, dir, configPath, addr, asIfID string) *testNode {
	t.Helper()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("bad address %q: %v", addr, err)
	}

	logPath := filepath.Join(dir, fmt.Sprintf("%s-%d.log", asIfID, time.Now().UnixNano()))
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create log file: %v", err)
	}
	t.Cleanup(func() { logFile.Close() })

	cmd := exec.Command(testBinPath, "-port=:"+port, "-id="+asIfID, "-config="+configPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start node %s: %v", asIfID, err)
	}

	return &testNode{addr: addr, logPath: logPath, cmd: cmd}
}

func (n *testNode) kill() {
	if n.cmd.Process != nil {
		n.cmd.Process.Kill()
		n.cmd.Wait()
	}
}

// status sends the STATUS command to addr and returns the trimmed reply, or "" on any error.
func status(addr string) string {
	conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
	if err != nil {
		return ""
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(300 * time.Millisecond))
	if _, err := conn.Write([]byte("STATUS\n")); err != nil {
		return ""
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return ""
	}
	return strings.TrimSpace(line)
}

// leaders returns the subset of addrs currently reporting role "leader".
func leaders(addrs []string) []string {
	var out []string
	for _, a := range addrs {
		if status(a) == "leader" {
			out = append(out, a)
		}
	}
	return out
}

// waitForSingleLeader polls until exactly one of addrs reports "leader".
func waitForSingleLeader(t *testing.T, addrs []string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ls := leaders(addrs); len(ls) == 1 {
			return ls[0]
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no single leader emerged among %v within %v", addrs, timeout)
	return ""
}

// wonElectionTerms scans a node's log for "won election for term N" lines.
func wonElectionTerms(t *testing.T, logPath string) []int {
	t.Helper()
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log %s: %v", logPath, err)
	}
	var terms []int
	for _, line := range strings.Split(string(data), "\n") {
		idx := strings.Index(line, "won election for term ")
		if idx == -1 {
			continue
		}
		var term int
		fmt.Sscanf(line[idx:], "won election for term %d", &term)
		terms = append(terms, term)
	}
	return terms
}

func threeNodeConfig(t *testing.T) (dir, configPath string, addrs map[string]string, addrList []string) {
	t.Helper()
	dir = t.TempDir()
	addrs = map[string]string{
		"node1": fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		"node2": fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		"node3": fmt.Sprintf("127.0.0.1:%d", freePort(t)),
	}
	configPath = writeConfig(t, dir, addrs)
	for _, addr := range addrs {
		addrList = append(addrList, addr)
	}
	return dir, configPath, addrs, addrList
}

func TestSingleLeaderElected(t *testing.T) {
	dir, configPath, addrs, addrList := threeNodeConfig(t)

	var nodes []*testNode
	for id, addr := range addrs {
		nodes = append(nodes, startNode(t, dir, configPath, addr, id))
	}
	defer func() {
		for _, n := range nodes {
			n.kill()
		}
	}()

	leader := waitForSingleLeader(t, addrList, 5*time.Second)
	time.Sleep(500 * time.Millisecond) // let things settle
	if ls := leaders(addrList); len(ls) != 1 || ls[0] != leader {
		t.Fatalf("leadership not stable: got %v, want exactly [%s]", ls, leader)
	}
}

// TestNoSplitBrainUnderLeaderChurn repeatedly kills and restarts the current
// leader (mirroring the manual "kill -9 the leader in a loop" repro used to
// find the TOCTOU election race) and asserts at most one leader is ever
// visible, and no term is won by more than one node cluster-wide.
func TestNoSplitBrainUnderLeaderChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow churn test in -short mode")
	}
	dir, configPath, addrs, addrList := threeNodeConfig(t)

	nodesByID := map[string]*testNode{}
	var allLogPaths []string
	for id, addr := range addrs {
		n := startNode(t, dir, configPath, addr, id)
		nodesByID[id] = n
		allLogPaths = append(allLogPaths, n.logPath)
	}
	defer func() {
		for _, n := range nodesByID {
			n.kill()
		}
	}()

	waitForSingleLeader(t, addrList, 5*time.Second)

	const rounds = 10
	for i := 0; i < rounds; i++ {
		ls := leaders(addrList)
		if len(ls) == 0 {
			time.Sleep(300 * time.Millisecond)
			continue
		}
		leaderAddr := ls[0]
		var leaderID string
		for id, addr := range addrs {
			if addr == leaderAddr {
				leaderID = id
			}
		}

		nodesByID[leaderID].kill()
		replacement := startNode(t, dir, configPath, leaderAddr, leaderID)
		nodesByID[leaderID] = replacement
		allLogPaths = append(allLogPaths, replacement.logPath)

		time.Sleep(300 * time.Millisecond)
		if ls := leaders(addrList); len(ls) > 1 {
			t.Fatalf("round %d: split brain detected, leaders at %v", i, ls)
		}
	}

	waitForSingleLeader(t, addrList, 5*time.Second)

	seenBy := map[int]int{}
	for _, logPath := range allLogPaths {
		for _, term := range wonElectionTerms(t, logPath) {
			seenBy[term]++
			if seenBy[term] > 1 {
				t.Fatalf("term %d was won by more than one node across the cluster", term)
			}
		}
	}
}

// TestSwappedIDsCauseSelfVoteSplitBrain reproduces the misconfiguration where
// an operator swaps which -id flag goes with which machine. The node bound
// to addr1 registers as "node2" and vice versa, so each node's peer list
// (built by excluding its own *id*, not its own *address*) ends up
// containing its own real address. That lets a candidate grant itself a
// second, phantom vote over a self-dial, reaching quorum alone.
//
// This currently FAILS: the self-vote bug is diagnosed but not yet fixed
// (see conversation/CHANGELOG). It's a regression test to turn green once
// REQUEST_VOTE rejects self-referential candidates and/or startup validates
// id-to-address ownership.
func TestSwappedIDsCauseSelfVoteSplitBrain(t *testing.T) {
	dir := t.TempDir()
	addr1 := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	addr2 := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	addr3 := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	addrs := map[string]string{"node1": addr1, "node2": addr2, "node3": addr3}
	configPath := writeConfig(t, dir, addrs)

	a := startNode(t, dir, configPath, addr1, "node2") // physically node1's addr, claims node2
	b := startNode(t, dir, configPath, addr2, "node1") // physically node2's addr, claims node1
	c := startNode(t, dir, configPath, addr3, "node3") // unaffected
	nodes := []*testNode{a, b, c}
	defer func() {
		for _, n := range nodes {
			n.kill()
		}
	}()

	// Poll continuously through the whole window rather than checking once
	// at the end: whether split brain occurs, and for how long, depends on
	// how the two swapped nodes' randomized election timeouts happen to
	// line up, so a single end-of-window snapshot can miss a transient hit.
	addrList := []string{addr1, addr2, addr3}
	deadline := time.Now().Add(6 * time.Second)
	var worst []string
	for time.Now().Before(deadline) {
		if ls := leaders(addrList); len(ls) > len(worst) {
			worst = ls
		}
		time.Sleep(100 * time.Millisecond)
	}

	if len(worst) > 1 {
		t.Fatalf("split brain from swapped ids: leaders at %v (each reached quorum via a phantom self-vote)", worst)
	}
}
