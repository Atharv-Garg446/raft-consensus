# Raft Consensus — TypeScript implementation

An in-memory Raft node built with Node.js, Express, and Axios. Implements the same protocol as the Rust version in this repo — see the root README for how the two compare and why both exist.

## Features

- **Leader election** — randomized election timers, candidate step-up, majority-vote counting, split-vote handling.
- **Log replication** — heartbeats, entry replication, log-matching and conflict resolution when a follower's log has diverged from the leader's.
- **State machine** — an in-memory key-value store, updated only once an entry is committed by a majority.
- **Client interface** — `/client/set` and `/client/get/:key`, with non-leaders replying `not_leader` so a client can find the current leader.
- **Diagnostics** — `/status` for role, term, log state, and the current key-value store contents.

## Installation

```bash
cd typescript
npm install
```

## Running tests

```bash
npm test
```

This runs the unit tests in `src/raft-logic.test.ts` against the pure logic in `src/raft-logic.ts` — the election-restriction check, log conflict resolution, and the commit-index safety rule. No server or cluster needed; they run in well under a second.

## Running a cluster locally

### Option 1: npm scripts (spins up several nodes at once)

```bash
npm run test:3node   # 3-node cluster
npm run test:5node   # 5-node cluster
```

### Option 2: one node at a time

```bash
npm run node1
npm run node2
npm run node3
```

### Option 3: shell scripts

```bash
./scripts/start-node1.sh
./scripts/start-node2.sh
./scripts/start-node3.sh
```

### Option 4: custom CLI args

```bash
npm run dev -- --id 1 --port 7001 --peers "2:127.0.0.1:7002,3:127.0.0.1:7003"
```

Once a cluster is running, watch the console output: nodes start as `Follower`, one times out and becomes `Candidate`, then wins the election and becomes `Leader`. Kill the leader's process (Ctrl+C in its terminal) and watch the remaining nodes elect a new one within a few hundred milliseconds.

## API endpoints

### Raft RPCs (internal)

**`POST /raft/request_vote`**
```json
{ "term": 1, "candidateId": 1, "lastLogIndex": 0, "lastLogTerm": 0 }
```
→
```json
{ "term": 1, "voteGranted": true }
```

**`POST /raft/append_entries`**
```json
{
  "term": 1,
  "leaderId": 1,
  "prevLogIndex": 0,
  "prevLogTerm": 0,
  "entries": [{ "term": 1, "index": 1, "key": 100, "value": "demo" }],
  "leaderCommit": 0
}
```
→
```json
{ "term": 1, "success": true }
```

### Client operations

**`POST /client/set`**
```json
{ "key": 42, "value": "hello world" }
```
→
```json
{ "status": "committed", "index": 1 }
```
(or `{ "status": "not_leader" }` if sent to a non-leader node)

**`GET /client/get/:key`**
→
```json
{ "found": true, "value": "hello world" }
```

**`GET /status`**
→
```json
{
  "id": 1,
  "role": "Leader",
  "term": 1,
  "lastLogIndex": 1,
  "lastLogTerm": 1,
  "commitIndex": 1,
  "lastApplied": 1,
  "store": { "42": "hello world" }
}
```

## Notes on this implementation

- State (log, term, role, key-value store) lives entirely in memory — restarting a node loses everything. A production implementation would need to persist the log and `votedFor` to disk before responding to RPCs, which is a safety requirement of the algorithm itself, not an optimization. See the root README for more on this tradeoff.
- Election and heartbeat timers are tuned for fast local testing (150–300ms election timeout, 50ms heartbeat interval); a deployment across a real network would need larger values.
