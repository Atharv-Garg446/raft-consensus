//! Pure Raft algorithm logic.
//!
//! Everything in this file is deliberately free of networking, locking, and
//! database code. `main.rs` wires these functions up to HTTP handlers and a
//! Mutex-guarded state struct, but the actual election-safety and log-matching
//! rules live here so they can be unit tested directly, without standing up
//! an HTTP server or a MongoDB connection.
//!
//! Section references (e.g. "§5.4.1") point at the Raft paper:
//! "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout).

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct LogEntry {
    pub term: u64,
    pub index: i64,
    pub key: i64,
    pub value: String,
}

/// Index of the last entry in the log, or 0 if the log is empty.
pub fn last_log_index(log: &[LogEntry]) -> i64 {
    log.last().map(|e| e.index).unwrap_or(0)
}

/// Term of the last entry in the log, or 0 if the log is empty.
pub fn last_log_term(log: &[LogEntry]) -> u64 {
    log.last().map(|e| e.term).unwrap_or(0)
}

/// Term of the entry at `index`, treating index 0 as the implicit "term 0"
/// entry before the log starts.
pub fn get_log_term(log: &[LogEntry], index: i64) -> Option<u64> {
    if index == 0 {
        return Some(0);
    }
    log.iter().find(|e| e.index == index).map(|e| e.term)
}

/// The election-restriction check from §5.4.1: a candidate's log is at least
/// as up to date as a voter's log if the candidate has a strictly later last
/// term, or the same last term and an equal-or-longer log. This is what stops
/// a node with stale data from getting elected leader and overwriting
/// committed entries.
pub fn is_log_up_to_date(
    candidate_last_term: u64,
    candidate_last_index: i64,
    voter_last_term: u64,
    voter_last_index: i64,
) -> bool {
    candidate_last_term > voter_last_term
        || (candidate_last_term == voter_last_term && candidate_last_index >= voter_last_index)
}

/// Quorum size for a cluster of `total_nodes` (including the caller itself).
pub fn majority(total_nodes: usize) -> usize {
    (total_nodes / 2) + 1
}

pub struct AppendResult {
    pub success: bool,
    pub conflict_index: Option<i64>,
    pub conflict_term: Option<u64>,
}

/// Applies one AppendEntries RPC to `log` in place, following the log-matching
/// and conflict-resolution rules from §5.3:
///
/// 1. If the follower doesn't have an entry at `prev_log_index` with term
///    `prev_log_term`, reject and tell the leader where to back up to.
/// 2. Otherwise, for each new entry: if the follower already has a
///    conflicting entry at that index (same index, different term), delete
///    it and everything after it, then append the leader's version.
/// 3. Existing entries that already match are left untouched, which is what
///    makes duplicate/late heartbeats safe to re-apply.
pub fn apply_append_entries(
    log: &mut Vec<LogEntry>,
    prev_log_index: i64,
    prev_log_term: u64,
    entries: &[LogEntry],
) -> AppendResult {
    if prev_log_index > 0 {
        match log.iter().find(|e| e.index == prev_log_index) {
            Some(e) if e.term != prev_log_term => {
                // We have something at that index, but it's from the wrong
                // term. Tell the leader the term we disagree on and the
                // earliest index where that term starts, so it can skip back
                // a whole term at a time instead of one entry at a time.
                let conflict_term = e.term;
                let conflict_index = log
                    .iter()
                    .find(|x| x.term == conflict_term)
                    .map(|x| x.index)
                    .unwrap_or(1);
                return AppendResult {
                    success: false,
                    conflict_index: Some(conflict_index),
                    conflict_term: Some(conflict_term),
                };
            }
            Some(_) => {} // matches — fall through and apply entries
            None => {
                // We don't have this index at all yet.
                return AppendResult {
                    success: false,
                    conflict_index: Some(last_log_index(log) + 1),
                    conflict_term: None,
                };
            }
        }
    }

    for entry in entries {
        if let Some(pos) = log.iter().position(|e| e.index == entry.index) {
            if log[pos].term != entry.term {
                log.truncate(pos);
                log.push(entry.clone());
            }
            // else: identical entry already present, nothing to do.
        } else {
            log.push(entry.clone());
        }
    }

    AppendResult {
        success: true,
        conflict_index: None,
        conflict_term: None,
    }
}

/// Given the leader's view of each follower's `match_index` (not including
/// itself), plus its own last log index, computes the highest index
/// replicated to a majority of the cluster — but only actually advances the
/// commit index if that entry was written in the leader's *current* term.
///
/// That last restriction is the subtle part of Raft (§5.4.2, Figure 8 in the
/// paper): a leader can only commit entries from previous terms indirectly,
/// by first committing an entry from its own term. Committing an old-term
/// entry just because it happens to be on a majority of servers can be undone
/// by a future leader and is a well-known source of bugs in naive
/// implementations.
pub fn compute_commit_index(
    mut match_indices: Vec<i64>,
    leader_last_log_index: i64,
    current_term: u64,
    log: &[LogEntry],
    current_commit_index: i64,
) -> i64 {
    match_indices.push(leader_last_log_index);
    match_indices.sort_by(|a, b| b.cmp(a));

    let majority_idx = (match_indices.len() - 1) / 2;
    let candidate = match_indices[majority_idx];

    if candidate > current_commit_index {
        if let Some(entry) = log.iter().find(|e| e.index == candidate) {
            if entry.term == current_term {
                return candidate;
            }
        }
    }
    current_commit_index
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(term: u64, index: i64) -> LogEntry {
        LogEntry {
            term,
            index,
            key: 0,
            value: String::new(),
        }
    }

    #[test]
    fn empty_log_reports_zero_index_and_term() {
        let log: Vec<LogEntry> = vec![];
        assert_eq!(last_log_index(&log), 0);
        assert_eq!(last_log_term(&log), 0);
        assert_eq!(get_log_term(&log, 0), Some(0));
        assert_eq!(get_log_term(&log, 5), None);
    }

    #[test]
    fn majority_matches_raft_quorum_for_common_cluster_sizes() {
        assert_eq!(majority(1), 1); // single node
        assert_eq!(majority(3), 2); // classic 3-node cluster
        assert_eq!(majority(5), 3);
        assert_eq!(majority(4), 3); // even-sized clusters round up
    }

    #[test]
    fn vote_granted_when_candidate_term_is_strictly_ahead() {
        assert!(is_log_up_to_date(5, 1, 4, 100));
    }

    #[test]
    fn vote_denied_when_same_term_but_shorter_log() {
        assert!(!is_log_up_to_date(4, 5, 4, 10));
    }

    #[test]
    fn vote_granted_when_same_term_and_equal_or_longer_log() {
        assert!(is_log_up_to_date(4, 10, 4, 10));
        assert!(is_log_up_to_date(4, 11, 4, 10));
    }

    #[test]
    fn append_entries_rejects_when_prev_log_entry_is_missing() {
        let mut log = vec![entry(1, 1)];
        let result = apply_append_entries(&mut log, 5, 1, &[]);
        assert!(!result.success);
        assert_eq!(result.conflict_index, Some(2)); // last_log_index + 1
        assert_eq!(log.len(), 1); // log untouched on rejection
    }

    #[test]
    fn append_entries_rejects_and_reports_conflict_term_on_mismatch() {
        let mut log = vec![entry(1, 1), entry(2, 2)];
        let result = apply_append_entries(&mut log, 2, 99, &[]);
        assert!(!result.success);
        assert_eq!(result.conflict_term, Some(2));
        assert_eq!(result.conflict_index, Some(2));
    }

    #[test]
    fn append_entries_truncates_conflicting_suffix_and_appends_leaders_entries() {
        let mut log = vec![entry(1, 1), entry(1, 2), entry(1, 3)];
        // Leader's entries from term 2 at index 2 conflict with our term-1 entry.
        let new_entries = vec![entry(2, 2), entry(2, 3)];
        let result = apply_append_entries(&mut log, 1, 1, &new_entries);
        assert!(result.success);
        assert_eq!(log.len(), 3);
        assert_eq!(log[1].term, 2);
        assert_eq!(log[2].term, 2);
    }

    #[test]
    fn append_entries_is_idempotent_for_a_repeated_heartbeat() {
        let mut log = vec![entry(1, 1)];
        let entries = vec![entry(1, 2)];
        apply_append_entries(&mut log, 1, 1, &entries);
        // Re-applying the exact same RPC (e.g. a retried heartbeat) must not
        // duplicate the entry.
        let result = apply_append_entries(&mut log, 1, 1, &entries);
        assert!(result.success);
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn commit_index_advances_when_a_current_term_entry_hits_majority() {
        let log = vec![entry(3, 1), entry(3, 2), entry(3, 3)];
        // Two followers at match_index 3 and 1; leader's own log is at 3.
        // Sorted descending: [3, 3, 1] -> majority index (len-1)/2 = 1 -> 3.
        let new_commit = compute_commit_index(vec![3, 1], 3, 3, &log, 0);
        assert_eq!(new_commit, 3);
    }

    #[test]
    fn commit_index_does_not_advance_on_a_previous_term_entry() {
        // This is the Figure 8 safety case from the Raft paper: even if an
        // old-term entry sits on a majority of servers, a leader must not
        // commit it just by counting replicas — only entries from its own
        // current term can be committed this way.
        let log = vec![entry(2, 1)]; // entry written in term 2
        let new_commit = compute_commit_index(vec![1, 1], 1, 3, &log, 0); // leader is now in term 3
        assert_eq!(new_commit, 0); // must NOT advance
    }

    #[test]
    fn commit_index_never_goes_backwards() {
        let log = vec![entry(3, 1), entry(3, 2)];
        let new_commit = compute_commit_index(vec![0, 0], 0, 3, &log, 2);
        assert_eq!(new_commit, 2); // stays put, doesn't regress
    }
}
