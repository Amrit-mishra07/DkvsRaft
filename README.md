# DkvsRaft

> A hand-rolled distributed key-value store built to actually understand consensus, proved out by breaking it on purpose.

![Go Version](https://img.shields.io/badge/go-%3E%3D1.21-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Raft](https://img.shields.io/badge/Raft-hand--rolled-orange.svg)
![Phases](https://img.shields.io/badge/Phases-5%2F5%20Complete-success.svg)

## 30-Second Pitch

DkvsRaft is an educational, from-scratch implementation of the Raft consensus algorithm in Go. It exists to deeply explore how distributed systems achieve strong consistency—without relying on third-party consensus libraries. Every safety property (leader election, log matching, quorum commits) is hand-built and empirically proven via fault-injection chaos testing. Kill the leader mid-write, and you can watch the remaining nodes elect a new one and seamlessly recover the cluster in under 300ms.

## Architecture at a Glance

```mermaid
graph TD
    Client((Client))

    subgraph "Cluster (Docker Compose)"
        Node1[Node 1 <br/> HTTP :8081 <br/> gRPC :50051]
        Node2[Node 2 <br/> HTTP :8082 <br/> gRPC :50052]
        Node3[Node 3 <br/> HTTP :8083 <br/> gRPC :50053]

        Node1 <-->|gRPC| Node2
        Node2 <-->|gRPC| Node3
        Node1 <-->|gRPC| Node3
    end

    Client -.->|POST /submit (307 Redirect)| Node2
    Client -->|POST /submit| Node1
    Client -->|GET /get| Node1
```

The system is built on a strict two-layer design:
1. **Consensus Layer (Raft)**: Exclusively handles leader election, heartbeat timeouts, log replication, and quorum tracking. To this layer, all commands are just opaque byte payloads.
2. **Application Layer (KV Store)**: A deterministic hashmap state machine. It is completely decoupled from the network and only applies state changes when the Raft layer explicitly feeds it a "committed" log entry.

Nodes talk to each other via **gRPC/Protobufs**, while exposing a lightweight HTTP API for client interactions.

## The 5 Guarantees This Project Proves

This isn't just theory. The codebase rigorously enforces and proves these invariants:

1. **Election Safety**: At most one leader can be elected in a given term, ever.
2. **Log Matching**: If two logs share an entry at the same index and term, every prior entry in both logs is identical. Conflict resolution automatically truncates divergent follower logs.
3. **State Machine Safety**: Once any node applies a command at a given log index, no node ever applies a different command at that index.
4. **Quorum Commits**: An entry is only "committed" after a majority of nodes have it durably recorded in their logs.
5. **Linearizable Reads**: The application layer never touches uncommitted entries, and reads are guarded by a synchronous heartbeat quorum check (`VerifyLeadership()`) to prevent stale reads from partitioned leaders.

## What's Inside — Phase by Phase

<details>
<summary><strong>Phase 1: Cluster Orchestration & Leader Election</strong></summary>

- **What was built**: Node state management (`Follower`, `Candidate`, `Leader`), `RequestVote` RPC implementation, randomized election timeouts (150-300ms), and heartbeat mechanisms.
- **Verification**: Ran a 3-node Docker Compose cluster. Killing the leader container triggered a new election within ~300ms. Validated via logs that distinct, increasing term numbers were correctly respected with no split-brain.
</details>

<details>
<summary><strong>Phase 2: State Machine Replication (Log Matching)</strong></summary>

- **What was built**: `Submit()` entrypoint, `nextIndex[]`/`matchIndex[]` tracking, and real log replication via `AppendEntries`. Implemented the strict Log Matching property where followers reject/truncate mismatched entries.
- **Verification**: Monitored leader logs actively advancing the `commitIndex` upon majority replication, while followers correctly appended, matched, and advanced their own commit indices safely behind the leader.
</details>

<details>
<summary><strong>Phase 3: Application State Machine (KV Store)</strong></summary>

- **What was built**: A deterministic `map[string]string` state machine. An async `applyLoop` goroutine strict-orders log entries and parses `SET` and `DEL` commands only when the `commitIndex` advances.
- **Verification**: Issued a `SET` command to the leader and independently queried a follower. Verified the follower had applied the write securely through the commit pipeline without ever receiving the raw client request.
</details>

<details>
<summary><strong>Phase 4: Client API & Cluster Routing</strong></summary>

- **What was built**: `HTTP 307 Temporary Redirect` logic abstraction. Followers automatically redirect client `GET` and `POST` requests to the active leader. Enforced Linearizable Reads by mandating a synchronous heartbeat (`VerifyLeadership()`) before the leader returns `GET` data.
- **Verification**: Sent requests to random follower nodes using `curl -L` and observed successful, seamless redirects to the leader. Verified the synchronous quorum check prevented stale data from being served.
</details>

<details>
<summary><strong>Phase 5: Resilience Testing & Chaos Engineering</strong></summary>

- **What was built**: A dedicated `test_chaos.sh` script to automate aggressive failure scenarios: isolating the leader mid-write, verifying re-elections, and bringing partitioned nodes back to observe log truncation and state recovery.
- **Verification**: Executed the chaos script against a live cluster. Confirmed successful failovers, no split-brain scenarios, and correct log truncation upon the revival of stale nodes.
</details>

## Quickstart

### Prerequisites
- Docker and Docker Compose
- *Optional*: Go 1.21+ (only needed if generating protobufs locally outside of Docker).

### 1. Boot the Cluster

First, generate the protobuf code using the isolated Docker environment, then start the nodes:

```bash
make proto-docker
docker-compose up --build
```

Watch the terminal logs. Within a few seconds, an election will conclude, and one node will output `Node [X] became LEADER`.

### 2. Interact with the API

You don't even need to know who the leader is. Use `curl -L` to follow the automatic 307 redirects from any node. 

Let's hit Node 2 (port 8082) with a write:
```bash
curl -L -X POST -d "SET status online" http://localhost:8082/submit
```

Now let's read it back from Node 3 (port 8083):
```bash
curl -L "http://localhost:8083/get?key=status"
```

## Go Break It Yourself (Interactive Chaos Demo)

The entire point of this project is fault tolerance. Don't take my word for it—break it yourself. 

We've bundled a script that runs a complete write -> kill leader -> write -> recover cycle. Open a new terminal window while your cluster is running and execute:

```bash
./test_chaos.sh
```

**What you'll see happen:**
1. A value is written to the cluster.
2. The active leader (`dkvsraft-node1-1`) is brutally killed via `docker stop`.
3. Within milliseconds, the remaining two nodes realize the leader is gone, initiate an election, and pick a new leader.
4. A new write is successfully committed by the remaining quorum.
5. The original leader is restarted. It wakes up, realizes its term is outdated, instantly steps down to a follower, and truncates its stale logs to match the new leader.

## Scope & Deliberate Non-Goals

To keep the implementation focused cleanly on the core mechanics of distributed consensus, several production features are intentionally out of scope:

- **Log compaction / snapshotting**: Logs grow indefinitely in memory.
- **Dynamic cluster membership**: The 3-node cluster is static. Joint consensus for rolling resizes is not implemented.
- **Disk-based persistence (WAL)**: The log is entirely in-memory. Crash durability applies to container restarts, but not host-level process termination.
- **TLS & Auth**: Network traffic is unencrypted, and endpoints are unauthenticated.

## Future Expansion

If this codebase were to evolve toward a production-grade system, these would be the most critical architectural next steps:

1. **Disk-Based Write-Ahead Log (WAL)**: Currently, state is lost if the entire cluster goes down. Implementing a persistent WAL (like `etcd`'s `wal` package) is the biggest missing piece for true durability.
2. **ReadIndex Optimization**: Right now, linearizable reads require a full `VerifyLeadership()` heartbeat round-trip. Implementing the ReadIndex protocol would allow the leader to serve reads securely without an active network round-trip, significantly cutting read latency.
3. **Log Compaction / Snapshotting**: To prevent unbounded memory growth, nodes need the ability to snapshot their state machine and discard historical log entries, seamlessly sending snapshots to slow followers.
4. **Dynamic Membership**: Implementing Raft's joint consensus would allow nodes to be safely added or removed without downtime.
5. **Observability**: Wiring up OpenTelemetry tracing across gRPC boundaries. Visualizing an `AppendEntries` RPC fanning out to followers in Jaeger would be an incredible teaching tool.

## How This Compares

This project is an educational sandbox, not a production database. Here is how its scope compares to real-world systems built on Raft:

| Feature | DkvsRaft | etcd | CockroachDB / TiKV |
| :--- | :--- | :--- | :--- |
| **Core Consensus** | Yes (Hand-rolled) | Yes | Yes (Multi-Raft) |
| **Storage** | In-Memory Hashmap | Disk (bbolt) | Disk (Pebble/RocksDB) |
| **Read Optimization** | Synchronous Heartbeat | ReadIndex / Lease | Leader Leases |
| **Cluster Topology** | Static (3 Nodes) | Dynamic (Joint Consensus) | Dynamic (Sharded) |
| **Durability** | Ephemeral | WAL to Disk | WAL to Disk |

## License

MIT
