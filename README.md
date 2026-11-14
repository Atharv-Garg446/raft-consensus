# Raft Consensus Algorithm

A Rust implementation of the Raft distributed consensus algorithm, built to actually understand how Raft works, not just read the paper.

Raft solves a specific problem: how do you get a cluster of machines to agree on a sequence of operations (like writes to a key-value store) even when some of the machines crash or the network drops messages, without any single machine's failure corrupting the data? It's the algorithm behind systems like etcd (which Kubernetes runs on) and Consul. I picked it because it's one of the few distributed systems algorithms designed from the ground up to be understandable rather than just provably correct — which made it a good target for "build it myself and see if I really get it."

## Repository structure

```text
.
├── rust/
│   ├── Cargo.toml
│   ├── README.md              # HTTP API reference + run instructions
│   └── src/
│       ├── main.rs            # HTTP handlers, background loops, state
│       └── raft_logic.rs      # Pure algorithm logic + unit tests
│
└── README.md
```

## Design decisions and tradeoffs

**HTTP + JSON instead of a binary RPC framework (gRPC, etc.).** Real Raft implementations (etcd, CockroachDB) use gRPC for performance and strongly-typed schemas. I used plain HTTP/JSON because it's inspectable with `curl` while debugging — I could watch exactly what a `RequestVote` RPC looked like on the wire without a proto compiler in the way. The tradeoff is real overhead (JSON parsing, no streaming) that a production system wouldn't accept.

**In-memory log, no persistence to disk.** A real Raft node has to persist its log and `voted_for` to disk before responding to RPCs, so that a crash-and-restart doesn't forget who it voted for or what it had committed — that's actually required for the algorithm's safety guarantees, not just an implementation detail. I left this out to keep the project scoped to the consensus logic itself (leader election, log replication, commit safety) rather than becoming a storage-engine project. It's the single biggest gap between this and a production-ready implementation, and I'd want to add write-ahead-log persistence as the next step if I extended this.

**Locking the whole state behind one mutex, instead of message-passing actors.** A single `Arc<Mutex<..>>` is simpler to reason about and avoids partial-update bugs, at the cost of serializing all access — every RPC handler and background loop takes the same lock. For a learning project favoring correctness over throughput, that tradeoff made sense.

**Pure logic separated from networking (`raft_logic.rs`).** The actual Raft rules — the election-restriction check, log-matching and conflict resolution, and the commit-index safety rule — live in a standalone file with no HTTP, no locks, no database calls. `main.rs` just wires them up to handlers. This is what makes it possible to unit test the parts of Raft that are easy to get subtly wrong (see below) without spinning up a cluster.

## The subtle part of Raft that's easy to get wrong

The one rule in Raft that isn't obvious from a first read of the paper: a leader can only advance its commit index by counting replicas for entries from **its own current term**. If an entry from an older term happens to be on a majority of servers, the leader still can't commit it directly — it has to wait until it's committed something from its current term first. This is Figure 8 in the original paper, and it exists because otherwise a later leader can end up silently overwriting an entry that an earlier leader considered "safely committed." `compute_commit_index` implements this, and it's directly covered by a test (`commit_index_does_not_advance_on_a_previous_term_entry`).

## Quick start

```bash
cd rust
cargo run -- --id 1 --port 7001 --peers "2:127.0.0.1:7002,3:127.0.0.1:7003"
```

Run unit tests:

```bash
cd rust
cargo test
```

See the [Rust README](rust/README.md) for the full HTTP API reference and multi-node startup commands.

## HTTP API

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/raft/request_vote` | `POST` | Internal RPC: candidate requests a vote during an election |
| `/raft/append_entries` | `POST` | Internal RPC: leader heartbeat and log replication |
| `/client/set` | `POST` | Client write; only the leader accepts this, others reply `not_leader` |
| `/client/get/:key` | `GET` | Client read from the state machine |
| `/status` | `GET` | Diagnostics: current role, term, log length, commit index, state machine contents |

## What's implemented

- **Leader election** — randomized election timeouts (150–300ms) to avoid split votes, the `Follower` → `Candidate` → `Leader` state machine, and majority-quorum vote counting.
- **Log replication** — `AppendEntries` RPCs with conflict detection so a leader can repair a follower whose log has diverged, and quorum-based commit index advancement (with the term-safety rule described above).
- **State machine** — a key-value store persisted to MongoDB, updated only once an entry is committed by a majority.

## What's not implemented (and would be next)

- Persistent storage of the log and voted-for state to disk (see "Design decisions" above).
- Cluster membership changes (adding/removing nodes without downtime).
- Log compaction / snapshotting, which real systems need once the log grows large.
