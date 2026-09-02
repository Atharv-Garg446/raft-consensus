/**
 * Pure Raft algorithm logic.
 *
 * Everything in this file is deliberately free of HTTP, timers, and process
 * state. index.ts wires these functions up to Express routes and the
 * RaftNode class, but the actual election-safety and log-matching rules live
 * here so they can be unit tested directly, without starting a server or
 * spinning up a multi-node cluster.
 *
 * Section references (e.g. "§5.4.1") point at the Raft paper:
 * "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout).
 */

export interface LogEntry {
  term: number;
  index: number;
  key: number;
  value: string;
}

/** Index of the last entry in the log, or 0 if the log is empty. */
export function lastLogIndex(log: LogEntry[]): number {
  const last = log[log.length - 1];
  return last ? last.index : 0;
}

/** Term of the last entry in the log, or 0 if the log is empty. */
export function lastLogTerm(log: LogEntry[]): number {
  const last = log[log.length - 1];
  return last ? last.term : 0;
}

/**
 * Term of the entry at `index`, treating index 0 as the implicit "term 0"
 * entry before the log starts.
 */
export function getLogTerm(log: LogEntry[], index: number): number | undefined {
  if (index === 0) return 0;
  return log.find((e) => e.index === index)?.term;
}

/**
 * The election-restriction check from §5.4.1: a candidate's log is at least
 * as up to date as a voter's log if the candidate has a strictly later last
 * term, or the same last term and an equal-or-longer log. This is what stops
 * a node with stale data from getting elected leader and overwriting
 * committed entries.
 */
export function isLogUpToDate(
  candidateLastTerm: number,
  candidateLastIndex: number,
  voterLastTerm: number,
  voterLastIndex: number
): boolean {
  return (
    candidateLastTerm > voterLastTerm ||
    (candidateLastTerm === voterLastTerm && candidateLastIndex >= voterLastIndex)
  );
}

/** Quorum size for a cluster of `totalNodes` (including the caller itself). */
export function majority(totalNodes: number): number {
  return Math.floor(totalNodes / 2) + 1;
}

export interface AppendResult {
  success: boolean;
  log: LogEntry[];
  conflictIndex?: number;
  conflictTerm?: number;
}

/**
 * Applies one AppendEntries RPC to `log`, following the log-matching and
 * conflict-resolution rules from §5.3:
 *
 * 1. If the follower doesn't have an entry at `prevLogIndex` with term
 *    `prevLogTerm`, reject and tell the leader where to back up to.
 * 2. Otherwise, for each new entry: if the follower already has a
 *    conflicting entry at that index (same index, different term), drop it
 *    and everything after it, then append the leader's version.
 * 3. Existing entries that already match are left untouched, which is what
 *    makes duplicate/late heartbeats safe to re-apply.
 *
 * Returns a *new* log array rather than mutating the input, so callers (and
 * tests) never have to worry about aliasing.
 */
export function applyAppendEntries(
  log: LogEntry[],
  prevLogIndex: number,
  prevLogTerm: number,
  entries: LogEntry[]
): AppendResult {
  const nextLog = [...log];

  if (prevLogIndex > 0) {
    const prevEntry = nextLog.find((e) => e.index === prevLogIndex);
    if (!prevEntry) {
      return {
        success: false,
        log: nextLog,
        conflictIndex: lastLogIndex(nextLog) + 1,
      };
    }
    if (prevEntry.term !== prevLogTerm) {
      const conflictTerm = prevEntry.term;
      const conflictIndex =
        nextLog.find((e) => e.term === conflictTerm)?.index ?? 1;
      return {
        success: false,
        log: nextLog,
        conflictIndex,
        conflictTerm,
      };
    }
  }

  for (const entry of entries) {
    const existingIndex = nextLog.findIndex((e) => e.index === entry.index);
    if (existingIndex >= 0) {
      const existing = nextLog[existingIndex];
      if (existing && existing.term !== entry.term) {
        nextLog.splice(existingIndex);
        nextLog.push(entry);
      }
      // else: identical entry already present, nothing to do.
    } else {
      nextLog.push(entry);
    }
  }

  return { success: true, log: nextLog };
}

/**
 * Given the leader's view of each follower's matchIndex (not including
 * itself), plus its own last log index, computes the highest index
 * replicated to a majority of the cluster — but only actually advances the
 * commit index if that entry was written in the leader's *current* term.
 *
 * That last restriction is the subtle part of Raft (§5.4.2, Figure 8 in the
 * paper): a leader can only commit entries from previous terms indirectly,
 * by first committing an entry from its own term. Committing an old-term
 * entry just because it happens to be on a majority of servers can be undone
 * by a future leader and is a well-known source of bugs in naive
 * implementations.
 */
export function computeCommitIndex(
  matchIndices: number[],
  leaderLastLogIndex: number,
  currentTerm: number,
  log: LogEntry[],
  currentCommitIndex: number
): number {
  const indices = [...matchIndices, leaderLastLogIndex].sort((a, b) => b - a);
  const candidate = indices[Math.floor((indices.length - 1) / 2)];

  if (candidate !== undefined && candidate > currentCommitIndex) {
    const entry = log.find((e) => e.index === candidate);
    if (entry && entry.term === currentTerm) {
      return candidate;
    }
  }
  return currentCommitIndex;
}
