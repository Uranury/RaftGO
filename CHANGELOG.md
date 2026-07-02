# Changelog

Rough log of what's been done, in order. Not pinned to versions since this is a learning project.

---

## Initial TCP server and client commands
- Set up a basic TCP listener that accepts connections and reads line-delimited commands.
- Implemented: `INCREMENT`, `VALUE`, `STATUS`, `HEALTH`, `HEARTBEAT`, `UPDATETIME`, `EXIT`.
- `sharedVar` is an in-memory integer that can be incremented. No replication yet.

## Connection reliability fixes
- Fixed a bug where a connection was being used before it was fully verified.
- Added handling for mid-connection teardown (half-open TCP connections).
- Added EOF handling so the server doesn't crash or spin on closed connections.

## Heartbeat and follower timeout
- Added a background ticker (200ms) that checks how long since the last heartbeat.
- If a non-leader node hasn't received a heartbeat in 500ms, it starts an election.
- `HEARTBEAT` command now resets `lastHeartbeat` timestamp.

## Node struct and refactor
- Moved all state into a `Node` struct (`id`, `role`, `term`, `votedFor`, `votes`, `peers`, `lastHeartbeat`, `stopLeader`).
- Converted standalone functions to methods on `Node`.
- Removed a pointless parameter that was being passed around unnecessarily.

## Multi-peer config via JSON
- Nodes now load their peer list from `config.json` (map of node ID → address).
- CLI flags: `--port`, `--role`, `--config`, `--id`.
- A node's own entry is excluded from its peer list at startup.

## Role type change
- Roles changed from iota constants to plain strings (`"leader"`, `"follower"`, `"candidate"`).
- Makes logs and debug output readable without a lookup table.

## Leader election
- Implemented `startElection()`: increments term, votes for self, sends `REQUEST_VOTE` to all peers in parallel goroutines.
- `REQUEST_VOTE term=<n> candidate=<id>` command added to the server.
- Nodes grant votes if the term is higher and they haven't voted yet this term.
- Majority quorum check: `votes*2 > len(peers)+1`.
- On winning, node transitions to leader and calls `startHeartbeats()`.

## Leader heartbeats
- `startHeartbeats()` spawns a goroutine per peer that sends `HEARTBEAT` every 200ms.
- Uses a `stopLeader` channel to stop heartbeat goroutines when the leader steps down.
- `stepDown(newTerm)` transitions back to follower, clears `votedFor`, and closes the stop channel.

## Bug fixes
- **Split brain attempt**: guarded vote-granting with a term check so a node can't grant votes for a stale term it already voted in.
- **Role handling fix**: corrected a case where role transitions weren't being applied under the lock correctly.
- **OS threads bug**: fixed a goroutine/scheduling issue where not enough OS threads were available — likely triggered by blocking calls in tight goroutine loops.
- **Zombie channel**: fixed a panic from closing an already-nil or already-closed `stopLeader` channel when `stepDown` was called more than once.

## Leader redirect and heartbeat term tracking
- Non-leader nodes now reject `INCREMENT` and return the current leader's address instead of executing locally.
- `HEARTBEAT` command extended to carry `term=<n> leader=<addr>` so followers can track the current leader and validate term authority.
- `startHeartbeats()` sends the enriched heartbeat format; followers update `leaderAddr` on each valid heartbeat.
- A node that receives a heartbeat from a higher-term leader automatically steps down and adopts the new term.

## Election timeout jitter and manual lock handling
- Added `electionTimeout` field to `Node`: randomised between 500–800ms on each reset to reduce split-vote probability.
- Added `resetElectionTimeout(min, max, t)` — called on heartbeat receipt, on granting a vote, and when a new election fires — so every path that should delay an election does so with a fresh random deadline.
- Removed per-method `sync.Mutex` from simple getters (`GetValue`, `Status`, `SetLeader`). Callers now hold the lock explicitly around command-dispatch blocks, making the locking discipline visible and preventing double-lock panics.

---

## What's not implemented yet

- **Log replication** — `AppendEntries` doesn't exist. The shared variable (`INCREMENT`) is not replicated across nodes; each node has its own independent copy.
- **Persistence** — no term or log durability across restarts.
- **Actual Raft log** — there are no log entries, no commit index, no state machine replay.
- **Heartbeat term verification** — the `HEARTBEAT` command resets the timer unconditionally; it doesn't check whether it came from the legitimate current-term leader.
- **`HEALTH`** always returns `1`. The commented-out client-side `healthCheck` function was removed.
