# Raft Consensus — TypeScript Implementation

A complete in-memory implementation of the **Raft Distributed Consensus Algorithm** in TypeScript using **Node.js**, **Express**, and **Axios**.

## Features

- **Leader Election**: Randomized election timers, candidate step-up, vote requests, split-vote handling.
- **Log Replication**: Heartbeats, log entry replication, majority consensus commit, log consistency checks and conflict resolution.
- **State Machine**: In-memory key-value store committed on quorum.
- **Client Interface**: Direct client requests (`/client/set`, `/client/get/:key`) with leader forwarding/redirection.
- **Diagnostics**: Real-time status endpoint (`/status`) showing role, term, logs, commit index, and state store.

## Installation

```bash
cd typescript
npm install
```

## Running Locally

### Option 1: Multi-node cluster using NPM scripts

```bash
# Run a 3-node cluster concurrently
npm run test:3node

# Or run a 5-node cluster concurrently
npm run test:5node
```

### Option 2: Individual nodes

```bash
# Node 1
npm run node1

# Node 2
npm run node2

# Node 3
npm run node3
```

### Option 3: Using helper shell scripts

```bash
./scripts/start-node1.sh
./scripts/start-node2.sh
./scripts/start-node3.sh
```

### Option 4: Custom CLI configuration

```bash
npm run dev -- --id 1 --port 7001 --peers "2:127.0.0.1:7002,3:127.0.0.1:7003"
```

## API Endpoints

### 1. Raft RPCs (Internal)

#### Request Vote
- **POST** `/raft/request_vote`
- **Body**:
  ```json
  {
    "term": 1,
    "candidateId": 1,
    "lastLogIndex": 0,
    "lastLogTerm": 0
  }
  ```
- **Response**:
  ```json
  {
    "term": 1,
    "voteGranted": true
  }
  ```

#### Append Entries / Heartbeat
- **POST** `/raft/append_entries`
- **Body**:
  ```json
  {
    "term": 1,
    "leaderId": 1,
    "prevLogIndex": 0,
    "prevLogTerm": 0,
    "entries": [
      { "term": 1, "index": 1, "key": 100, "value": "demo" }
    ],
    "leaderCommit": 0
  }
  ```
- **Response**:
  ```json
  {
    "term": 1,
    "success": true
  }
  ```

### 2. Client Operations

#### Set Key-Value
- **POST** `/client/set`
- **Body**:
  ```json
  {
    "key": 42,
    "value": "hello world"
  }
  ```
- **Response**:
  ```json
  {
    "status": "committed",
    "index": 1
  }
  ```

#### Get Value by Key
- **GET** `/client/get/:key`
- **Response**:
  ```json
  {
    "found": true,
    "value": "hello world"
  }
  ```

#### Cluster Status
- **GET** `/status`
- **Response**:
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
