# raft_rust — HTTP API reference

Minimal in-memory Raft node, built with async Rust (Tokio + Axum). This document covers the HTTP endpoints, request/response shapes, and how to run a cluster locally.

## Running a node

```bash
cargo run -- --id <NODE_ID> --port <PORT> --peers "id1:host1:port1,id2:host2:port2,..."
```

### Local 3-node cluster

```bash
# terminal 1
cargo run -- --id 1 --port 7001 --peers "2:127.0.0.1:7002,3:127.0.0.1:7003"

# terminal 2
cargo run -- --id 2 --port 7002 --peers "1:127.0.0.1:7001,3:127.0.0.1:7003"

# terminal 3
cargo run -- --id 3 --port 7003 --peers "1:127.0.0.1:7001,2:127.0.0.1:7002"
```

Watch any node's logs and you'll see it start as a `Follower`, one node time out and become a `Candidate`, then win the election and become `Leader`. Kill the leader's process and watch a new election happen within a few hundred milliseconds.

### Running tests

```bash
cargo test
```

This runs the unit tests in `src/raft_logic.rs`, which cover the election-restriction check, log conflict resolution, and the commit-index safety rule — the parts of Raft that are easiest to get subtly wrong. They run in milliseconds since they test pure functions directly, with no HTTP server or database involved.

## Endpoints

All endpoints use JSON. Replace `127.0.0.1:7001` with your node's address.

### `POST /raft/request_vote`

Internal RPC. A candidate calls this on its peers during an election.

Request:
```json
{
  "term": 2,
  "candidate_id": 1,
  "last_log_index": 10,
  "last_log_term": 2
}
```

Response:
```json
{
  "term": 2,
  "vote_granted": true
}
```

```bash
curl -X POST http://127.0.0.1:7002/raft/request_vote \
  -H 'Content-Type: application/json' \
  -d '{"term":2,"candidate_id":1,"last_log_index":10,"last_log_term":2}'
```

### `POST /raft/append_entries`

Internal RPC. The leader calls this on followers for both heartbeats (empty `entries`) and log replication.

Request:
```json
{
  "term": 3,
  "leader_id": 1,
  "prev_log_index": 10,
  "prev_log_term": 2,
  "entries": [
    { "term": 3, "index": 11, "key": 42, "value": "hello" }
  ],
  "leader_commit": 11
}
```

Response:
```json
{
  "term": 3,
  "success": true,
  "conflict_index": null,
  "conflict_term": null
}
```

On rejection, `conflict_index` / `conflict_term` tell the leader where to back up its `next_index` for that follower, so it doesn't have to retry one entry at a time.

Heartbeat example (no entries):
```bash
curl -X POST http://127.0.0.1:7002/raft/append_entries \
  -H 'Content-Type: application/json' \
  -d '{"term":3,"leader_id":1,"prev_log_index":0,"prev_log_term":0,"entries":[],"leader_commit":5}'
```

### `POST /client/set`

Client write. Must be sent to the current leader — a non-leader replies `not_leader` and the client is expected to retry against a different node.

Request:
```json
{ "key": 42, "value": "some string" }
```

Response (committed):
```json
{ "status": "committed", "index": 11 }
```

Response (wrong node):
```json
{ "status": "not_leader", "index": null }
```

Response (key already present):
```json
{ "status": "Key already exists", "index": null }
```

Response (didn't commit within ~1 second):
```json
{ "status": "timeout", "index": 11 }
```

```bash
curl -X POST http://127.0.0.1:7001/client/set \
  -H 'Content-Type: application/json' \
  -d '{"key":42,"value":"hello"}'
```

The leader appends the entry to its own log, then waits (polling every 50ms, up to 1 second) for it to be replicated to a majority before replying `committed`. Note this implementation treats keys as write-once — a `set` on an existing key is rejected rather than overwritten. The TypeScript implementation in this repo does not have this restriction; see the root README's "Where the two implementations actually differ" section.

### `GET /client/get/:key`

Reads a key from MongoDB, where committed entries are persisted once applied. Any node can serve this — it's not restricted to the leader.

```bash
curl http://127.0.0.1:7001/client/get/42
```

Response:
```json
{ "found": true, "value": "some string" }
```

### `GET /status`

Diagnostics for a single node.

```bash
curl http://127.0.0.1:7001/status
```

```json
{
  "id": 1,
  "role": "Leader",
  "term": 3,
  "last_log_index": 12,
  "last_log_term": 3,
  "commit_index": 12,
  "last_applied": 12,
  "log_length": 12,
  "store": { "42": "hello" }
}
```

## Notes on this implementation

- Raft *state* (log, term, role) is kept in memory only — restarting a node loses it. A real implementation has to persist the log and `voted_for` to disk before responding to RPCs; that's a safety requirement of the algorithm, not just a nice-to-have. See the root README's "Design decisions" section for more on this tradeoff.
- Once an entry is committed, it's applied to a MongoDB-backed key-value store, which is what `/client/get` reads from.
- Election and heartbeat timers (150–300ms election timeout, 50ms heartbeat interval) are tuned for fast local testing. Over a real network with higher latency, these would need to be larger to avoid spurious elections.
