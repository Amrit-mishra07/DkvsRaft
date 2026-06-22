# DkvsRaft (Distributed Key-Value Store with Raft)

A fault-tolerant, strictly consistent, distributed key-value store built in Go. It uses the **Raft Consensus Algorithm** to maintain State Machine Replication (SMR) across a cluster of nodes, ensuring high availability and consistency in the face of network partitions and node crashes.

This project was built from the ground up to demonstrate deep understanding of distributed systems principles without relying on third-party consensus libraries.

## 🚀 Key Features & Architecture

* **Leader Election & Heartbeats:** Randomized election timeouts prevent split-brain scenarios. A strong leader handles all cluster coordination.
* **State Machine Replication (Log Matching):** Distributed consensus ensures that all nodes apply operations in the exact same order. The Log Matching property guarantees safely truncating uncommitted divergence.
* **Intelligent Client Routing:** Clients can query or write to *any* node in the cluster. Followers intercept requests and issue `HTTP 307 Temporary Redirects` to the current leader seamlessly.
* **Strict Linearizable Reads:** The leader verifies its quorum synchronously before serving `GET` requests, guaranteeing that a silently partitioned leader never serves stale data.
* **Fault Tolerance (2f + 1):** The 3-node cluster can survive `f=1` node failure (or network isolation) without downtime.

## 🛠️ Technology Stack
* **Language:** Go (Golang)
* **RPC:** gRPC / Protobuf
* **Containerization:** Docker & Docker Compose

## 🏃‍♂️ How to Run

### 1. Start the Cluster
```bash
sudo docker compose up --build
```
This boots up three nodes (`node1`, `node2`, `node3`) with HTTP APIs available on ports `8081`, `8082`, and `8083`.

### 2. Write Data
You can post data to *any* node. (If it's a follower, your request is redirected to the leader).
```bash
curl -L -X POST -d "SET name amrit" http://localhost:8081/submit
```

### 3. Read Data
Read data from *any* node. The cluster guarantees linearizability.
```bash
curl -L "http://localhost:8082/get?key=name"
```

## 💥 Chaos Engineering & Resilience Testing

To prove the system is fault-tolerant, you can simulate a node crash:
1. Identify the current leader from the Docker logs.
2. Stop the leader: `sudo docker stop dkvsraft-node1-1`
3. Watch the logs: within ~300ms, the remaining two nodes will detect missing heartbeats, increment the term, and elect a new leader.
4. Continue writing/reading data to the new cluster. No data is lost.
5. Restart the dead node: `sudo docker start dkvsraft-node1-1`. It will rejoin as a follower, truncate any uncommitted divergent logs, and catch up to the current state machine.

A helper script `test_chaos.sh` is included to automate testing this fault-tolerance sequence.
