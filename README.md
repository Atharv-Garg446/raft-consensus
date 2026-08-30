# Raft Consensus Algorithm

A multi-language repository containing implementations of the **Raft Distributed Consensus Algorithm** in **Rust** and **TypeScript**.

Raft is a consensus algorithm designed to be understandable and fault-tolerant, equivalent to Paxos in fault-tolerance and performance while decomposing consensus into key elements such as leader election, log replication, and safety.

---

## 📁 Repository Structure

```text
.
├── rust/                      # Rust Raft Implementation
│   ├── Cargo.toml
│   ├── Cargo.lock
│   ├── README.md
│   └── src/
│       └── main.rs
│
├── typescript/                # TypeScript Raft Implementation
│   ├── package.json
│   ├── package-lock.json
│   ├── tsconfig.json
│   ├── README.md
│   ├── scripts/
│   │   ├── start-node1.sh
│   │   ├── start-node2.sh
│   │   └── start-node3.sh
│   └── src/
│       ├── index.ts
│       └── types.ts
│
├── .gitignore
└── README.md                  # Root documentation
```

---

## 🚀 Quick Start

### 1. Rust Implementation (`rust/`)

Built with asynchronous Rust using **Tokio**, **Axios/Reqwest/Hyper/Warp/Axum**, and **Serde**.

```bash
cd rust

# Run Node 1
cargo run -- --id 1 --port 7001 --peers "2:127.0.0.1:7002,3:127.0.0.1:7003"

# Run Node 2
cargo run -- --id 2 --port 7002 --peers "1:127.0.0.1:7001,3:127.0.0.1:7003"

# Run Node 3
cargo run -- --id 3 --port 7003 --peers "1:127.0.0.1:7001,2:127.0.0.1:7002"
```

For full details, see the [Rust Implementation README](rust/README.md).

---

### 2. TypeScript Implementation (`typescript/`)

Built with **Node.js**, **Express**, **Axios**, and **Commander**.

```bash
cd typescript
npm install

# Run 3-node cluster concurrently
npm run test:3node

# Or run nodes individually
npm run node1
npm run node2
npm run node3
```

For full details, see the [TypeScript Implementation README](typescript/README.md).

---

## 📡 Common HTTP API Endpoints

Both implementations expose compatible HTTP REST endpoints for consensus RPCs and client interaction:

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/raft/request_vote` | `POST` | Internal RPC for candidate election vote requests |
| `/raft/append_entries` | `POST` | Internal RPC for leader heartbeat and log replication |
| `/client/set` | `POST` | Client key-value write operation (commits across quorum) |
| `/client/get/:key` | `GET` | Client key-value read operation from state machine |
| `/status` | `GET` | Node diagnostics (current role, term, log, state machine) |

---

## 💡 Key Raft Concepts Implemented

1. **Leader Election**:
   - Randomized election timeout intervals (150ms – 300ms) to avoid split votes.
   - Transition between `Follower`, `Candidate`, and `Leader` roles.
   - Majority quorum voting verification before stepping up as Leader.

2. **Log Replication & Consistency**:
   - Leader appends client entries to its log and sends `AppendEntries` RPCs to followers.
   - Quorum-based commit index advancement.
   - Log repair and conflict detection for lagging or divergent nodes.

3. **State Machine Execution**:
   - In-memory key-value store applied strictly upon log entry commitment.
   - Safe reads and atomic state updates.
