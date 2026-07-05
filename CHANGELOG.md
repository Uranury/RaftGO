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

## No more redundant elections
- `startElection()` now bails out immediately if the node is already `leader`, instead of tearing down its own leadership by re-running for election.
- Fixed a data race in `startHeartbeats()`: the heartbeat sender goroutine now reads `n.term`/`n.addr` under the lock when building the `HEARTBEAT` message, instead of reading `n.term` unsynchronized.
- Collapsed the election-timeout ticker's read-then-unlock-then-relock sequence into a single critical section per tick (still followed by a second, separate fix below — this pass removed the pointless double lock/unlock but didn't yet close the real race).

## Concurrency fix: election decision and state mutation in one lock acquisition
- Root-caused a TOCTOU race in the election ticker: it read `role`/`elapsed`/`timeout` and decided to start an election, then released the lock and re-acquired it separately in `startElection()` to mutate state. In the gap between those two lock acquisitions, a concurrent `REQUEST_VOTE` or `HEARTBEAT` handler could legitimately update `votedFor`/`lastHeartbeat` (e.g. granting a vote to another candidate) — only for the ticker's stale decision to clobber it by bumping the term and re-declaring candidacy anyway.
- Split `startElection()` into `beginElection()` (pure state mutation — term++, role=candidate, votedFor=self; must be called with `n.mu` already held) and `broadcastVoteRequests(term)` (the network fan-out, called lock-free). The ticker now holds one lock across the read, the decision, and the mutation, and only releases it right before the network broadcast.
- Verified with a 3-node cluster across repeated leader-kill cycles: no more spurious re-elections clobbering a just-granted vote.
- **Found separately, not yet fixed**: an operator swapping which `-id` flag goes with which machine causes each swapped node's peer list (built by excluding its own *id*, not its own *address*) to include its own real address. A candidate then dials itself, and its own `REQUEST_VOTE` handler grants it a second, phantom vote (since `votedFor == candidate` trivially holds for a self-referential request) — letting a single misconfigured node reach quorum alone. This produces genuine split brain (two leaders, same term) and is a different bug from the TOCTOU race above: it's deterministic, not timing-dependent. Reproduced in `cluster_test.go` (`TestSwappedIDsCauseSelfVoteSplitBrain`, currently a known failing/red test). Fix would be to reject self-referential `REQUEST_VOTE`s and/or validate id-to-address ownership at startup.

---

## What's not implemented yet

- **Log replication** — `AppendEntries` doesn't exist. The shared variable (`INCREMENT`) is not replicated across nodes; each node has its own independent copy.
- **Persistence** — no term or log durability across restarts.
- **Actual Raft log** — there are no log entries, no commit index, no state machine replay.
- **Heartbeat term verification** — the `HEARTBEAT` command resets the timer unconditionally; it doesn't check whether it came from the legitimate current-term leader.
- **`HEALTH`** always returns `1`. The commented-out client-side `healthCheck` function was removed.
- **Self-vote via id/address mismatch** — a node never checks whether an incoming `REQUEST_VOTE`'s candidate is itself, and peers are excluded by config *id* rather than by *address*. Swapping which `-id` a machine runs as puts that machine's own address in its own peer list, letting it phantom-vote for itself twice and reach quorum without any real peer. See `cluster_test.go`.
