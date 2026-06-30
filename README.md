# RaftGO

Personal implementation of the Raft consensus algorithm from scratch in Go.
If you're reading this, please make sure to follow me on GitHub and LinkedIn. I will keep working on similar interesting concepts 😊

## What's working

- TCP server per node, accepting line-delimited commands
- Leader election: timeout detection, `REQUEST_VOTE`, majority quorum, term tracking
- Leader heartbeats: winner sends `HEARTBEAT` to all peers every 200ms
- Step-down: a node reverts to follower when it sees a higher term
- Multi-node config loaded from JSON

## What's not working yet

- **Log replication** — the `INCREMENT` command only modifies the local node's in-memory integer. Nothing is replicated.
- **Persistence** — state is lost on restart.
- **Leader redirect** — any node accepts any command regardless of role.
- **Heartbeat validation** — heartbeats aren't verified to come from the current-term leader.

## Layout

```
main.go       — Node struct, TCP listener, command dispatch, election timeout loop
commands.go   — Node methods: election, heartbeats, vote requests, step-down, config loading
config.json   — node ID → address map
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
| `INCREMENT` | Increment the local shared integer |
| `VALUE` | Return current value of the shared integer |
| `STATUS` | Return node's current role (leader/follower/candidate) |
| `HEALTH` | Returns `1` (always) |
| `HEARTBEAT` | Reset the heartbeat timer (sent by leader to peers) |
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
