
# AGENT.md — Distributed KV Store with Raft Consensus

This file gives coding agents (Claude Code, Antigravity CLI, etc.) the context
needed to work on this repo correctly. Read this fully before making changes.
Update the "CURRENT PHASE" section as the project progresses — do not let it
go stale.

================================================================================
## PROJECT OVERVIEW (stable — do not change)
================================================================================

A distributed key-value store in Go implementing the Raft consensus algorithm
from scratch. Goal: a CP system (per CAP theorem) — strongly consistent,
remains available as long as a quorum of nodes is reachable, tolerates node
crashes and network partitions.

Two layers, kept strictly separate:
- **Consensus layer (Raft)**: leader election, log replication, commit index
  tracking. Treats all commands as opaque byte payloads. Knows nothing about
  what a "PUT" or "GET" means.
- **Application layer (KV store)**: a deterministic hashmap state machine.
  Only applies a command after the consensus layer marks it committed.

Communication between nodes: gRPC + Protocol Buffers.
Local multi-node orchestration: Docker Compose.
Tests: Go's built-in `testing` package, including failure-injection tests.

## NON-NEGOTIABLE INVARIANTS

Any code the agent writes or modifies must preserve these. If a change would
violate one, stop and flag it instead of proceeding:

1. **Election Safety** — at most one leader per term, ever.
2. **Log Matching** — if two logs share an entry at the same index and term,
   every prior entry in both logs is identical.
3. **State Machine Safety** — once any node applies a command at a given log
   index, no node may ever apply a different command at that index.
4. **Commit requires quorum** — an entry is only "committed" after a majority
   of nodes (not just the leader) have it durably in their log.
5. **Application layer never touches uncommitted entries.** The KV hashmap is
   only updated from the commit pipeline, never directly from a client
   request or an uncommitted log append.

## SCOPE BOUNDARIES — DO NOT IMPLEMENT UNLESS EXPLICITLY ASKED

These are intentionally excluded from this project. If asked to "improve" or
"productionize" something, do not silently add these:
- Log compaction / snapshotting
- Dynamic cluster membership changes (joint consensus)
- Disk-based persistence / WAL (in-memory log is intentional for this scope)
- TLS, auth, or access control

## CODING CONVENTIONS

- Go, idiomatic style, `gofmt`-clean.
- One goroutine per logical responsibility per node (election timer, heartbeat
  sender, RPC server) — avoid ad hoc goroutine sprawl.
- All shared mutable state (log, commitIndex, currentTerm, votedFor) guarded
  by a single node-level mutex unless there's a specific, documented reason
  to split locks.
- gRPC service definitions live in `.proto` files; regenerate, don't
  hand-edit generated code.
- Every Raft state transition (Follower→Candidate, Candidate→Leader, etc.)
  should log the transition with term number — this is essential for
  debugging election issues later.

================================================================================
## PHASE 1 — Cluster Orchestration & Leader Election
================================================================================
**Goal:** 3 nodes that elect a leader and survive a leader crash.

Agent should focus on:
- Define `NodeState` enum: Follower, Candidate, Leader.
- Define gRPC service with `RequestVote` RPC (no `AppendEntries` yet).
- Implement randomized election timeout (e.g. 150–300ms) — randomization is
  required to avoid repeated split votes; do not use a fixed timeout.
- Implement heartbeat mechanism (empty `AppendEntries` is fine as a stub here
  if needed, but full log replication is NOT in scope for this phase).
- `RequestVote` must check: is candidate's term ≥ mine, and is candidate's
  log at least as up-to-date as mine, before granting a vote.
- Each node tracks `currentTerm` and `votedFor`, reset/updated correctly on
  every term change.

**Definition of done for this phase:**
- 3 nodes started via Docker Compose elect exactly one leader.
- Killing the leader container results in a new leader being elected within
  a few seconds, with no split-brain (verify via logs showing term numbers).

**Do not start Phase 2 work until this is solid and tested.**

================================================================================
## PHASE 2 — State Machine Replication (Log Matching)
================================================================================
**Goal:** Leader replicates log entries to followers with correctness
guarantees, and tracks commit index by quorum.

Agent should focus on:
- Extend `AppendEntries` RPC to carry real log entries (index, term, command).
- Implement the Log Matching check: a follower rejects an `AppendEntries` if
  the entry at `prevLogIndex` doesn't match `prevLogTerm` — this is the
  mechanism that keeps logs consistent across nodes after a leader change.
- Leader tracks `nextIndex[]` and `matchIndex[]` per follower.
- Leader advances `commitIndex` only when a majority of `matchIndex[]` values
  are ≥ that index.
- Do NOT apply committed entries to any state machine yet — that's Phase 3.
  This phase is purely about the log itself being correctly replicated.

**Definition of done for this phase:**
- Write a test that appends entries via the leader and verifies all 3 nodes'
  logs converge to the identical sequence.
- Write a test that kills a follower mid-replication, restarts it, and
  verifies its log catches up correctly via the Log Matching check.

================================================================================
## PHASE 3 — Application State Machine (KV Store)
================================================================================
**Goal:** A hashmap per node that applies commands strictly in commit order.

Agent should focus on:
- Background loop per node that watches `commitIndex` advancing and applies
  the corresponding log entries to a local `map[string]string`, in strict
  index order (no skipping, no out-of-order application).
- Command encoding/decoding (PUT/DELETE as the opaque byte payload the Raft
  layer carries — keep this layer ignorant of Raft internals and vice versa).
- This layer must be a pure function of "committed log so far" — given the
  same committed log, every node's hashmap must end up identical.

**Definition of done for this phase:**
- After a sequence of PUTs through the leader, all 3 nodes' hashmaps are
  byte-for-byte identical.
- Killing and restarting a follower mid-stream results in its hashmap
  eventually converging to match the others (after log catch-up).

================================================================================
## PHASE 4 — Client API & Cluster Routing
================================================================================
**Goal:** A usable client-facing API with correct leader routing and reads.

Agent should focus on:
- Expose `PUT` / `GET` / `DELETE` over gRPC (or REST if simpler) on every
  node.
- If a non-leader receives a write, it must redirect/reject with the current
  leader's address — never silently accept a write it can't safely commit.
- **Linearizable reads**: before answering a `GET`, the leader should confirm
  it is still the current leader (e.g. via a quick heartbeat round) so it
  never serves stale data after losing leadership. Do not implement naive
  reads straight from any node's local hashmap without this check.

**Definition of done for this phase:**
- A CLI client can PUT/GET/DELETE against any node in the cluster and get
  correct results regardless of which node it initially contacts.
- A `GET` issued to a stale/partitioned former leader does not return wrong
  data (test this explicitly).

================================================================================
## PHASE 5 — Resilience Testing & Chaos Engineering
================================================================================
**Goal:** Prove the safety guarantees hold under adverse conditions, not
just the happy path.

Agent should focus on:
- Test: drop a follower's network connection mid-replication, verify cluster
  continues with quorum.
- Test: isolate the leader (simulate partition) — verify it cannot commit
  new writes (no quorum) while the majority partition elects a new leader
  and continues serving.
- Test: delayed/out-of-order RPC delivery — verify term checks and log
  matching prevent corruption.
- Test: partitioned former leader rejoins — verify its stale, uncommitted
  entries are correctly overwritten/truncated per Raft rules, never
  silently kept.

**This phase is optional given time constraints — even one solid chaos test
(e.g. leader isolation) is worth more than rushing extra unrelated features.**

================================================================================
## CURRENT PHASE — update this section as you go
================================================================================
**Active phase:** Phase 1
**Status:** Not yet started
**Blocking issues:** none yet

(Agent: when resuming work, read this section first to know exactly where to
pick up. Update it before ending a session.)