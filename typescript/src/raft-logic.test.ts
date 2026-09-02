import { describe, it, expect } from "vitest";
import {
  LogEntry,
  lastLogIndex,
  lastLogTerm,
  getLogTerm,
  isLogUpToDate,
  majority,
  applyAppendEntries,
  computeCommitIndex,
} from "./raft-logic";

function entry(term: number, index: number): LogEntry {
  return { term, index, key: 0, value: "" };
}

describe("log helpers", () => {
  it("report zero index and term for an empty log", () => {
    expect(lastLogIndex([])).toBe(0);
    expect(lastLogTerm([])).toBe(0);
    expect(getLogTerm([], 0)).toBe(0);
    expect(getLogTerm([], 5)).toBeUndefined();
  });
});

describe("majority", () => {
  it("matches Raft quorum sizes for common cluster sizes", () => {
    expect(majority(1)).toBe(1); // single node
    expect(majority(3)).toBe(2); // classic 3-node cluster
    expect(majority(5)).toBe(3);
    expect(majority(4)).toBe(3); // even-sized clusters round up
  });
});

describe("isLogUpToDate (election restriction, §5.4.1)", () => {
  it("grants when the candidate's term is strictly ahead", () => {
    expect(isLogUpToDate(5, 1, 4, 100)).toBe(true);
  });

  it("denies when same term but a shorter log", () => {
    expect(isLogUpToDate(4, 5, 4, 10)).toBe(false);
  });

  it("grants when same term and an equal-or-longer log", () => {
    expect(isLogUpToDate(4, 10, 4, 10)).toBe(true);
    expect(isLogUpToDate(4, 11, 4, 10)).toBe(true);
  });
});

describe("applyAppendEntries (log matching + conflict resolution, §5.3)", () => {
  it("rejects when prevLogIndex is missing from the log", () => {
    const result = applyAppendEntries([entry(1, 1)], 5, 1, []);
    expect(result.success).toBe(false);
    expect(result.conflictIndex).toBe(2); // lastLogIndex + 1
    expect(result.log).toEqual([entry(1, 1)]); // log left untouched
  });

  it("rejects and reports the conflicting term on a term mismatch", () => {
    const result = applyAppendEntries([entry(1, 1), entry(2, 2)], 2, 99, []);
    expect(result.success).toBe(false);
    expect(result.conflictTerm).toBe(2);
    expect(result.conflictIndex).toBe(2);
  });

  it("truncates a conflicting suffix and appends the leader's entries", () => {
    const log = [entry(1, 1), entry(1, 2), entry(1, 3)];
    // Leader's entries from term 2 at index 2 conflict with our term-1 entry.
    const newEntries = [entry(2, 2), entry(2, 3)];
    const result = applyAppendEntries(log, 1, 1, newEntries);
    expect(result.success).toBe(true);
    expect(result.log).toHaveLength(3);
    expect(result.log[1]!.term).toBe(2);
    expect(result.log[2]!.term).toBe(2);
  });

  it("is idempotent for a repeated heartbeat", () => {
    const afterFirst = applyAppendEntries([entry(1, 1)], 1, 1, [entry(1, 2)]);
    // Re-applying the exact same RPC (e.g. a retried heartbeat) must not
    // duplicate the entry.
    const afterSecond = applyAppendEntries(afterFirst.log, 1, 1, [entry(1, 2)]);
    expect(afterSecond.success).toBe(true);
    expect(afterSecond.log).toHaveLength(2);
  });

  it("does not mutate the input log (returns a new array)", () => {
    const original = [entry(1, 1)];
    const result = applyAppendEntries(original, 1, 1, [entry(1, 2)]);
    expect(original).toHaveLength(1); // unchanged
    expect(result.log).toHaveLength(2);
  });
});

describe("computeCommitIndex (commit safety rule, §5.4.2 / Figure 8)", () => {
  it("advances when a current-term entry hits a majority", () => {
    const log = [entry(3, 1), entry(3, 2), entry(3, 3)];
    // Two followers at matchIndex 3 and 1; leader's own log is at 3.
    const newCommit = computeCommitIndex([3, 1], 3, 3, log, 0);
    expect(newCommit).toBe(3);
  });

  it("refuses to commit an entry from a previous term via replica counting", () => {
    // The classic Figure 8 safety case: even if an old-term entry sits on a
    // majority of servers, the leader must not commit it just by counting
    // replicas — only entries from its own current term can be committed
    // this way.
    const log = [entry(2, 1)]; // entry written in term 2
    const newCommit = computeCommitIndex([1, 1], 1, 3, log, 0); // leader is now in term 3
    expect(newCommit).toBe(0); // must NOT advance
  });

  it("never regresses the commit index", () => {
    const log = [entry(3, 1), entry(3, 2)];
    const newCommit = computeCommitIndex([0, 0], 0, 3, log, 2);
    expect(newCommit).toBe(2); // stays put
  });
});
