# RaftGO

Personal implementation of the Raft consensus algorithm from scratch in Go.
If you're reading this, please make sure to follow me on GitHub and LinkedIn. I will keep working on similar interesting concepts 😊

## What's working

- TCP server per node, accepting line-delimited commands
- Leader election: timeout detection, `REQUEST_VOTE`, majority quorum, term tracking
- Leader heartbeats: winner sends `HEARTBEAT` to all peers every 200ms
- Step-down: a node reverts to follower when it sees a higher term
- Multi-node config loaded from JSON
- Election-timeout ticker no longer races itself: the "should I run for election" decision and the state mutation that starts one (`beginElection`) happen under a single lock acquisition, so a node can't clobber a vote it just granted by re-declaring candidacy on stale info
- A leader no longer tears down its own leadership by re-running for election against itself
- Naive state replication: on `INCREMENT`, the leader applies it locally, then fires `REPLICATE_INCREMENT` at every peer so their copy of `sharedVar` catches up

## What's not working yet

- **Real log replication** — there's no Raft log, no `AppendEntries`, no commit index. `REPLICATE_INCREMENT` is a fire-and-forget broadcast: the leader doesn't wait for acks, doesn't retry on failure, and doesn't require a quorum before treating the increment as committed. A peer that's down or partitioned when the broadcast goes out just silently misses the update and never catches up.
- **Persistence** — state is lost on restart.
- **Heartbeat validation** — heartbeats check the sender's term but don't verify the sender's identity beyond that.
- **Self-vote via id/address mismatch** — peers are excluded by config *id*, not by *address*. If a node is started with the wrong `-id` for the machine it's running on (e.g. two node ids swapped between two machines), its own address ends up in its own peer list, and nothing stops it granting itself a second vote over that self-dial — reaching quorum alone. Reproduced in `cluster_test.go` (`TestSwappedIDsCauseSelfVoteSplitBrain`), not yet fixed.

## Layout

```
main.go           — Node struct, TCP listener, command dispatch, election timeout loop
commands.go       — Node methods: election (beginElection/broadcastVoteRequests), heartbeats, vote requests, step-down, replication (replicateIncrement), config loading
cluster_test.go   — integration tests that spin up real raftgo processes to check for split brain
config.json       — node ID → address map
```

## Running

Each node needs a unique `--id` that matches a key in `config.json`, a `--port` to listen on, and optionally a starting `--role`.

```sh
go run . --id node1 --port :9000
go run . --id node2 --port :9001
go run . --id node3 --port :9002
```

You can connect manually with `nc` or `telnet`:

```
nc localhost 9000
```

## Commands

| Command | Description |
|---|---|
| `INCREMENT` | Leader-only: increment the shared integer locally, then broadcast `REPLICATE_INCREMENT` to all peers. Non-leaders reply with the current leader's address instead of executing it |
| `REPLICATE_INCREMENT` | Increment the local shared integer; sent by the leader to peers, not meant to be called directly |
| `VALUE` | Return current value of the shared integer |
| `STATUS` | Return node's current role (leader/follower/candidate) |
| `HEALTH` | Returns `1` (always) |
| `HEARTBEAT term=<n> leader=<addr>` | Reset the election timer; validates sender's term and tracks current leader address |
| `UPDATETIME` | Return the timestamp of the last heartbeat |
| `REQUEST_VOTE term=<n> candidate=<id>` | Request a vote for an election |
| `EXIT` | Close the connection |

## Config

`config.json` maps node IDs to `host:port` addresses. A node excludes itself from its peer list automatically.

```json
{
  "node1": "192.168.10.4:9000",
  "node2": "192.168.1.103:9000",
  "node3": "192.168.1.101:9000"
}
```
