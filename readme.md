cargo run --release -- --id 1 --port 7001 --peers "2:192.168.138.252:7002,3:192.168.137.144:7003,4:192.168.137.131:7004,5:183.82.105.187:7005"

cargo run --release -- --id 2 --port 7002 --peers "1:192.168.137.145:7001,3:192.168.137.144:7003,4:192.168.137.131:7004,5:183.82.105.187:7005"

cargo run --release -- --id 3 --port 7003 --peers "1:192.168.137.145:7001,2:192.168.138.252:7002,4:192.168.137.131:7004,5:183.82.105.187:7005"

cargo run --release -- --id 4 --port 7004 --peers "1:192.168.137.145:7001,2:192.168.138.252:7002,3:192.168.137.144:7003,5:183.82.105.187:7005"

cargo run --release -- --id 5 --port 7005 --peers "1:192.168.137.145:7001,2:192.168.138.252:7002,3:192.168.137.144:7003,4:192.168.137.131:7004"



//for local testing

# Node 1
cargo run --release -- --id 1 --port 7001 --peers "2:127.0.0.1:7002,3:127.0.0.1:7003,4:127.0.0.1:7004,5:127.0.0.1:7005"

# Node 2
cargo run --release -- --id 2 --port 7002 --peers "1:127.0.0.1:7001,3:127.0.0.1:7003,4:127.0.0.1:7004,5:127.0.0.1:7005"

# Node 3
cargo run --release -- --id 3 --port 7003 --peers "1:127.0.0.1:7001,2:127.0.0.1:7002,4:127.0.0.1:7004,5:127.0.0.1:7005"

# Node 4
cargo run --release -- --id 4 --port 7004 --peers "1:127.0.0.1:7001,2:127.0.0.1:7002,3:127.0.0.1:7003,5:127.0.0.1:7005"

# Node 5
cargo run --release -- --id 5 --port 7005 --peers "1:127.0.0.1:7001,2:127.0.0.1:7002,3:127.0.0.1:7003,4:127.0.0.1:7004"


# raft_rust — HTTP API Reference

This document describes the HTTP endpoints implemented by this minimal in-memory Raft node, the JSON schemas for requests and responses, and example curl commands.

Run the node

```sh
cargo run -- --id <NODE_ID> --port <PORT> --peers "host1:port1,host2:port2,..."
```

All endpoints use JSON. Replace `127.0.0.1:7001` with your node address.

Endpoints
---------

**POST /raft/request_vote**

- Purpose: Candidate -> request votes from peers during election.
- Request body (RequestVoteArgs):

```json
{
	"term": 2,
	"candidate_id": 1,
	"last_log_index": 10,
	"last_log_term": 2
}
```

- Response body (RequestVoteResponse):

```json
{
	"term": 2,
	"vote_granted": true
}
```

Example:

```sh
curl -X POST http://127.0.0.1:7002/raft/request_vote \
	-H 'Content-Type: application/json' \
	-d '{"term":2,"candidate_id":1,"last_log_index":10,"last_log_term":2}'
```

---

**POST /raft/append_entries**

- Purpose: Leader -> send heartbeats and replicate log entries.
- Request body (AppendEntriesArgs):

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

- Response body (AppendEntriesResponse):

```json
{
	"term": 3,
	"success": true,
	"conflict_index": null,
	"conflict_term": null
}
```

Heartbeat example (no entries):

```sh
curl -X POST http://127.0.0.1:7002/raft/append_entries \
	-H 'Content-Type: application/json' \
	-d '{"term":3,"leader_id":1,"prev_log_index":0,"prev_log_term":0,"entries":[],"leader_commit":5}'
```

---

**POST /client/set**

- Purpose: Client API to set a key/value. Should be sent to the leader; non-leaders reply `not_leader`.
- Request body (ClientSet):

```json
{
	"key": 42,
	"value": "some string"
}
```

- Response body (ClientSetResp):

If committed:

```json
{
	"status": "committed",
	"index": 11
}
```

If node is not leader:

```json
{
	"status": "not_leader",
	"index": null
}
```

Example:

```sh
curl -X POST http://127.0.0.1:7001/client/set \
	-H 'Content-Type: application/json' \
	-d '{"key":42,"value":"hello"}'
```

Notes: The leader appends the entry and attempts to replicate it to a majority. The implementation may wait briefly before reporting commit.

---

**GET /client/get/:key**

- Purpose: Read the value for a key from the node's committed state machine.
- Path parameter: `:key` (integer)
- Response body (ClientGetResp):

If present:

```json
{
	"found": true,
	"value": "some string"
}
```

If not present:

```json
{
	"found": false,
	"value": null
}
```

Example:

```sh
curl http://127.0.0.1:7001/client/get/42
```

---

**GET /status**

- Purpose: Diagnostic endpoint returning node status and in-memory store.
- Response example:

```json
{
	"id": 1,
	"role": "Leader",
	"term": 3,
	"last_log_index": 12,
	"last_log_term": 3,
	"commit_index": 12,
	"last_applied": 12,
	"store": { "42": "hello" }
}
```

Example:

```sh
curl http://127.0.0.1:7001/status
```

Implementation notes
--------------------
- The node keeps state in memory only (no durable persistence).
- Timers are tuned for local testing — increase timeouts for real networks.
- `append_entries` supports basic conflict detection; followers may return `conflict_index` and `conflict_term` to help the leader repair logs.

If you'd like, I can also add a small Postman collection or sample Rust client to exercise these endpoints.

