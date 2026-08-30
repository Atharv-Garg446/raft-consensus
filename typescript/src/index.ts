import express from "express";
import { randomInt } from "crypto";
import axios from "axios";
import { program } from "commander";

enum Role {
  Follower = "Follower",
  Candidate = "Candidate",
  Leader = "Leader",
}

interface LogEntry {
  term: number;
  index: number;
  key: number;
  value: string;
}

interface RequestVoteArgs {
  term: number;
  candidateId: number;
  lastLogIndex: number;
  lastLogTerm: number;
}

interface RequestVoteResponse {
  term: number;
  voteGranted: boolean;
}

interface AppendEntriesArgs {
  term: number;
  leaderId: number;
  prevLogIndex: number;
  prevLogTerm: number;
  entries: LogEntry[];
  leaderCommit: number;
}

interface AppendEntriesResponse {
  term: number;
  success: boolean;
  conflictIndex?: number;
  conflictTerm?: number;
}

interface ClientSet {
  key: number;
  value: string;
}

// interface ClientSetResp {
//   status: string;
//   index?: number;
// }

// interface ClientGetResp {
//   found: boolean;
//   value?: string;
// }

interface PeerInfo {
  id: number;
  addr: string;
}

class RaftState {
  id: number;
  role: Role = Role.Follower;
  currentTerm: number = 0;
  votedFor?: number;
  log: LogEntry[] = [];
  commitIndex: number = 0;
  lastApplied: number = 0;
  nextIndex: Map<number, number> = new Map();
  matchIndex: Map<number, number> = new Map();
  peers: PeerInfo[];
  stateMachine: Map<number, string> = new Map();
  lastHeartbeatMs: number = Date.now();

  constructor(id: number, peers: PeerInfo[]) {
    this.id = id;
    this.peers = peers;
  }

  lastLogIndex(): number {
    const last = this.log[this.log.length - 1];
    return last ? last.index : 0;
  }

  lastLogTerm(): number { 
    const last = this.log[this.log.length - 1];
    return last ? last.term : 0;
  }

  getLogTerm(index: number): number | undefined {  
    if (index === 0) return 0;
    return this.log.find((e) => e.index === index)?.term;
  }

  majority(): number { 
    return Math.floor((this.peers.length + 2) / 2);
  }
}

class RaftNode {
  private state: RaftState;
  private app = express();
  private httpClient = axios.create({ timeout: 100 });

  constructor(id: number, port: number, peers: PeerInfo[]) {
    this.state = new RaftState(id, peers);
    this.setupRoutes();
    this.startBackgroundTasks();

    this.app.listen(port, () => {
      console.log(`Node ${id} listening on port ${port}`);
    });
  }

  private setupRoutes(): void {
    this.app.use(express.json());

    this.app.post("/raft/request_vote", (req, res) =>
      this.handleRequestVote(req, res)
    );
    this.app.post("/raft/append_entries", (req, res) =>
      this.handleAppendEntries(req, res)
    );
    this.app.post("/client/set", (req, res) => this.handleClientSet(req, res));
    this.app.get("/client/get/:key", (req, res) =>
      this.handleClientGet(req, res)
    );
    this.app.get("/status", (req, res) => this.handleStatus(req, res));
  }

  private async handleRequestVote(
    req: express.Request,
    res: express.Response
  ): Promise<void> {
    const args: RequestVoteArgs = req.body;

    if (args.term < this.state.currentTerm) {
      res.json({ term: this.state.currentTerm, voteGranted: false });
      return;
    }

    if (args.term > this.state.currentTerm) {
      this.state.currentTerm = args.term;
      this.state.votedFor = undefined;
      this.state.role = Role.Follower;
    }

    const upToDate =
      args.lastLogTerm > this.state.lastLogTerm() ||
      (args.lastLogTerm === this.state.lastLogTerm() &&
        args.lastLogIndex >= this.state.lastLogIndex());

    let voteGranted = false;
    if (
      (this.state.votedFor === undefined ||
        this.state.votedFor === args.candidateId) &&
      upToDate
    ) {
      this.state.votedFor = args.candidateId;
      this.state.lastHeartbeatMs = Date.now();
      console.log(
        `Node ${this.state.id} voted for ${args.candidateId} in term ${this.state.currentTerm}`
      );
      voteGranted = true;
    }

    res.json({ term: this.state.currentTerm, voteGranted });
  }

  private async handleAppendEntries(
    req: express.Request,
    res: express.Response
  ): Promise<void> {
    const args: AppendEntriesArgs = req.body;

    if (args.term < this.state.currentTerm) {
      res.json({ term: this.state.currentTerm, success: false });
      return;
    }

    if (args.term >= this.state.currentTerm) {
      this.state.currentTerm = args.term;
      this.state.role = Role.Follower;
      this.state.votedFor = undefined;
    }

    this.state.lastHeartbeatMs = Date.now();

    if (args.prevLogIndex > 0) { 
      const prevEntry = this.state.log.find(
        (e) => e.index === args.prevLogIndex
      );
      if (!prevEntry || prevEntry.term !== args.prevLogTerm) {
        res.json({
          term: this.state.currentTerm,
          success: false,
          conflictIndex: prevEntry
            ? prevEntry.index
            : this.state.lastLogIndex() + 1,
          conflictTerm: prevEntry?.term,
        });
        return;
      }
    }

    for (const entry of args.entries) {
      const existingIndex = this.state.log.findIndex(
        (e) => e.index === entry.index
      );
      if (existingIndex >= 0) {
        const existingEntry = this.state.log[existingIndex];
        if (existingEntry && existingEntry.term !== entry.term) {
          this.state.log.splice(existingIndex);
          this.state.log.push(entry);
        }
      } else {
        this.state.log.push(entry);
      }
    }

    this.state.commitIndex = Math.min(
      args.leaderCommit,
      this.state.lastLogIndex()
    );
    res.json({ term: this.state.currentTerm, success: true });
  }

  private async handleClientSet(
    req: express.Request,
    res: express.Response
  ): Promise<void> {
    const reqData: ClientSet = req.body;

    if (this.state.role !== Role.Leader) {
      res.json({ status: "not_leader", index: undefined });
      return;
    }

    const index = this.state.lastLogIndex() + 1;
    const entry: LogEntry = {
      term: this.state.currentTerm,
      index: index,
      key: reqData.key,
      value: reqData.value,
    };
    this.state.log.push(entry);

    for (let i = 0; i < 20; i++) {
      await this.delay(50);
      if (this.state.commitIndex >= index) {
        res.json({ status: "committed", index });
        return;
      }
    }

    res.json({ status: "timeout", index });
  }

  private async handleClientGet(
    req: express.Request,
    res: express.Response
  ): Promise<void> {
    const keyParam = req.params.key;
    if (typeof keyParam !== "string") {
      res.status(400).json({ found: false });
      return;
    }

    const key = parseInt(keyParam, 10);
    if (Number.isNaN(key)) {
      res.status(400).json({ found: false });
      return;
    }

    const value = this.state.stateMachine.get(key);
    res.json({ found: value !== undefined, value });
  }

  private async handleStatus(
    req: express.Request,
    res: express.Response
  ): Promise<void> {
    const storeObj: Record<number, string> = {};
    this.state.stateMachine.forEach((value, key) => {
      storeObj[key] = value;
    });

    res.json({
      id: this.state.id,
      role: this.state.role,
      term: this.state.currentTerm,
      votedFor: this.state.votedFor,
      lastLogIndex: this.state.lastLogIndex(),
      lastLogTerm: this.state.lastLogTerm(),
      commitIndex: this.state.commitIndex,
      lastApplied: this.state.lastApplied,
      logLength: this.state.log.length,
      store: storeObj,
    });
  }

  private startBackgroundTasks(): void {
    this.electionLoop();
    this.applyCommittedLoop();
    this.leaderHeartbeatLoop();
  }

  private async electionLoop(): Promise<void> {
    while (true) {
      await this.delay(randomInt(150, 300));

      if (this.state.role === Role.Leader) continue;

      const elapsed = Date.now() - this.state.lastHeartbeatMs;
      if (elapsed < 300) continue;

      // Start election
      this.state.role = Role.Candidate;
      this.state.currentTerm += 1;
      this.state.votedFor = this.state.id;
      this.state.lastHeartbeatMs = Date.now();

      console.log(
        `Node ${this.state.id} starting election for term ${this.state.currentTerm}`
      );

      const requests = this.state.peers.map((peer) => {
        const args: RequestVoteArgs = {
          term: this.state.currentTerm,
          candidateId: this.state.id,
          lastLogIndex: this.state.lastLogIndex(),
          lastLogTerm: this.state.lastLogTerm(),
        };

        return this.httpClient
          .post<RequestVoteResponse>(
            `http://${peer.addr}/raft/request_vote`,
            args
          )
          .then((r) => r.data)
          .catch(() => null);
      });

      const results = await Promise.all(requests);
      let votes = 1;
      let higherTerm = 0;

      for (const result of results) {
        if (!result) continue;
        if (result.term > this.state.currentTerm) {
          higherTerm = Math.max(higherTerm, result.term);
        } else if (result.voteGranted) {
          votes++;
        }
      }

      if (higherTerm > 0) {
        this.state.currentTerm = higherTerm;
        this.state.role = Role.Follower;
        this.state.votedFor = undefined;
        continue;
      }

      if (votes >= this.state.majority()) {
        this.state.role = Role.Leader;
        console.log(
          `Node ${this.state.id} became leader for term ${this.state.currentTerm}`
        );

        const nextIdx = this.state.lastLogIndex() + 1;
        this.state.peers.forEach((peer) => {
          this.state.nextIndex.set(peer.id, nextIdx);
          this.state.matchIndex.set(peer.id, 0);
        });
      } else {
        this.state.role = Role.Follower;
      }
    }
  }

  private async leaderHeartbeatLoop(): Promise<void> {
    while (true) {
      await this.delay(50);
      if (this.state.role !== Role.Leader) continue;

      this.state.peers.forEach((peer) => {
        this.replicateToPeer(peer);
      });
    }
  }

  private async replicateToPeer(peer: PeerInfo): Promise<void> {
    const nextIdx = this.state.nextIndex.get(peer.id) || 1;
    const entries = this.state.log.filter((e) => e.index >= nextIdx);
    const prevLogIndex = nextIdx - 1;
    const prevLogTerm = this.state.getLogTerm(prevLogIndex) || 0;

    const args: AppendEntriesArgs = {
      term: this.state.currentTerm,
      leaderId: this.state.id,
      prevLogIndex,
      prevLogTerm,
      entries,
      leaderCommit: this.state.commitIndex,
    };

    try {
      const response = await this.httpClient.post<AppendEntriesResponse>(
        `http://${peer.addr}/raft/append_entries`,
        args
      );

      const result = response.data;
      if (result.term > this.state.currentTerm) {
        this.state.currentTerm = result.term;
        this.state.role = Role.Follower;
        this.state.votedFor = undefined;
        return;
      }

      if (result.success && entries.length > 0) {
        const lastEntry = entries[entries.length - 1];
        if (lastEntry) {
          const lastIndex = lastEntry.index;
          this.state.nextIndex.set(peer.id, lastIndex + 1);
          this.state.matchIndex.set(peer.id, lastIndex);
          this.updateCommitIndex();
        }
      } else if (!result.success) {
        this.state.nextIndex.set(peer.id, Math.max(nextIdx - 1, 1));
      }
    } catch (error) {
      // Request failed - will retry
    }
  }

  private updateCommitIndex(): void {
    if (this.state.role !== Role.Leader) return;

    const indices = [
      ...this.state.matchIndex.values(),
      this.state.lastLogIndex(),
    ];
    indices.sort((a, b) => b - a);
    const n = indices[Math.floor((indices.length - 1) / 2)];

    if (n !== undefined && n > this.state.commitIndex) {
      const entry = this.state.log.find((e) => e.index === n);
      if (entry && entry.term === this.state.currentTerm) {
        this.state.commitIndex = n;
        console.log(`Leader ${this.state.id} advanced commit_index to ${n}`);
      }
    }
  }

  private async applyCommittedLoop(): Promise<void> {
    while (true) {
      while (this.state.lastApplied < this.state.commitIndex) {
        const next = this.state.lastApplied + 1;
        const entry = this.state.log.find((e) => e.index === next);
        if (entry) {
          this.state.stateMachine.set(entry.key, entry.value);
          this.state.lastApplied = next;
          console.log(`Node ${this.state.id} applied entry at index ${next}`);
        } else {
          break;
        }
      }
      await this.delay(50);
    }
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// CLI and startup
program
  .option("--id <number>", "Node ID", (value) => parseInt(value, 10))
  .option("--port <number>", "Port number", (value) => parseInt(value, 10))
  .option("--peers <string>", "Comma separated peer entries: id:host:port")
  .parse();

const options = program.opts();

if (!options.id || !options.port) {
  console.error("Missing required arguments: --id and --port");
  process.exit(1);
}

const peers: PeerInfo[] = (options.peers || "")
  .split(",")
  .filter(Boolean)
  .map((peerStr: { split: (arg0: string) => [any, any, any]; }) => {
    const [id, host, port] = peerStr.split(":");
    return {
      id: parseInt(id, 10),
      addr: `${host}:${port}`,
    };
  });

// Start the node
new RaftNode(options.id, options.port, peers);
