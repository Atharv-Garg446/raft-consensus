// export interface RequestVoteRPC {
//   from: string;
//   to: string;
//   term: number;
//   lastLogIndex: number;
//   lastLogTerm: number;
// }

// export interface RequestVoteResponse {
//   from: string;
//   to: string;
//   term: number;
//   granted: boolean;
// }

// export interface AppendEntriesRPC {
//   from: string;
//   to: string;
//   term: number;
//   prevLogIndex: number;
//   prevLogTerm: number;
//   entries: LogEntry[];
//   leaderCommit: number;
// }

// export interface AppendEntriesResponse {
//   from: string;
//   to: string;
//   term: number;
//   success: boolean;
// }

// export interface LogEntry {
//   term: number;
//   command: any;
// }

// export interface NodeConfig {
//   id: string;
//   port: number;
//   peers: string[];
// }

// export enum NodeState {
//   FOLLOWER = "follower",
//   CANDIDATE = "candidate",
//   LEADER = "leader",
// }
