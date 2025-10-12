use std::collections::BTreeSet;

use proptest_derive::Arbitrary;

use super::*;
use crate::message::*;
use crate::state::*;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Arbitrary)]
struct Cmd(i32);
impl Command for Cmd {}

#[derive(Default)]
struct TestStateMachine {
    applied: Vec<Cmd>,
}
impl StateMachine<Cmd> for TestStateMachine {
    fn apply(&mut self, entry: &Cmd) {
        self.applied.push(entry.clone());
    }
}

#[derive(Default)]
struct TestStorage;
impl Storage<Cmd> for TestStorage {
    fn persist_state(
        _current_term: Option<Term>,
        _voted_for: Option<Option<NodeId>>,
        _log: &[Cmd],
    ) {
    }
}

fn new_node<const N: usize>(id: NodeId) -> RaftCore<N, Cmd, TestStateMachine, TestStorage> {
    RaftCore::new(id, TestStateMachine::default(), TestStorage)
}

fn log_entry(term: Term, cmd: i32) -> LogEntry<Cmd> {
    LogEntry {
        term,
        command: Cmd(cmd),
    }
}

// Reject AppendEntries RPCs with a lower term.
#[test]
fn append_entries_request_rejects_lower_term() {
    const N: usize = 3;
    let node_id = 1;
    let node_term = 5;
    let mut node = new_node::<N>(node_id);
    node.log_state.set_term(node_term);

    let leader_id = 0;
    let req = AppendEntriesRequest {
        term: 4,
        leader_id,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
    };

    let mut out = node
        .handle_message(Message::AppendEntriesRequest(req))
        .unwrap();
    assert_eq!(out.len(), 1);
    let (resp_node_id, msg) = out.remove(0);
    let AppendEntriesResponse {
        term,
        success,
        node_id: out_node_id,
        next_index,
    } = msg.unwrap_append_entries_response();
    assert_eq!(resp_node_id, leader_id);
    assert_eq!(term, node_term);
    assert!(!success);
    assert_eq!(out_node_id, node_id);
    assert_eq!(next_index, None);
}

// Reject AppendEntries if prev_log is missing or term mismatches.
#[test]
fn append_entries_request_rejects_missing_or_mismatch() {
    const N: usize = 3;
    let node_id = 2;
    let node_term = 2;
    let mut node = new_node::<N>(node_id);
    node.log_state.set_term(node_term);

    {
        // Missing prev log.
        let leader_id = 0;
        let req = AppendEntriesRequest {
            term: node_term,
            leader_id,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 0,
        };

        let mut out = node
            .handle_message(Message::AppendEntriesRequest(req))
            .unwrap();
        assert_eq!(out.len(), 1);
        let (resp_node_id, msg) = out.remove(0);
        let AppendEntriesResponse {
            term,
            success,
            node_id: out_node_id,
            next_index,
        } = msg.unwrap_append_entries_response();
        assert_eq!(resp_node_id, leader_id);
        assert_eq!(term, node_term);
        assert!(!success);
        assert_eq!(out_node_id, node_id);
        assert_eq!(next_index, None);
    }

    {
        // Mismatched term.
        let leader_id = 0;
        // index 1, term 3.
        let log_term = 3;
        node.log_state.log.push(log_entry(log_term, 10));
        let req = AppendEntriesRequest {
            term: log_term,
            leader_id,
            prev_log_index: 1,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 0,
        };

        let mut out = node
            .handle_message(Message::AppendEntriesRequest(req))
            .unwrap();
        assert_eq!(out.len(), 1);
        let (resp_node_id, msg) = out.remove(0);
        let AppendEntriesResponse {
            term,
            success,
            node_id: out_node_id,
            next_index,
        } = msg.unwrap_append_entries_response();
        assert_eq!(resp_node_id, leader_id);
        assert!(!success);
        assert_eq!(term, log_term);
        assert_eq!(out_node_id, node_id);
        assert_eq!(next_index, None);

        assert_eq!(*node.log_state.current_term(), log_term);
    }
}

// Receiving AppendEntries of the same term makes a candidate step down to follower.
#[test]
fn append_entries_request_same_term_makes_candidate_follower() {
    const N: usize = 3;
    let node_id = 0;
    let node_term = 1;
    let mut node = new_node::<N>(node_id);
    node.log_state.set_term(node_term);
    node.role = RoleState::Candidate { votes: 0 };

    // Receiving AppendEntries with the same term should convert to follower and accept.
    let leader_id = 1;
    let req = AppendEntriesRequest {
        term: node_term,
        leader_id,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
    };

    let mut out = node
        .handle_message(Message::AppendEntriesRequest(req))
        .unwrap();
    assert_eq!(out.len(), 1);
    let (resp_node_id, msg) = out.remove(0);
    let AppendEntriesResponse {
        term,
        success,
        node_id: out_node_id,
        next_index,
    } = msg.unwrap_append_entries_response();
    assert_eq!(resp_node_id, leader_id);
    assert_eq!(term, node_term);
    assert!(success);
    assert_eq!(out_node_id, node_id);
    assert_eq!(next_index, Some(NonZeroIndex::new(1).unwrap()));

    assert_eq!(node.role.role(), Role::Follower);
    assert_eq!(node.node_state.leader_id, Some(leader_id));
}

// Conflicting entries are truncated; new entries appended; commit and apply follow the leader.
#[test]
fn append_entries_request_conflict_truncate_append_commit_apply() {
    const N: usize = 3;
    let node_id = 1;
    let node_term = 3;
    let mut node = new_node::<N>(node_id);
    node.log_state.set_term(node_term);
    // Existing log: [(t1, 1), (t2, 2), (t3, 3)].
    node.log_state
        .log
        .extend([log_entry(1, 1), log_entry(2, 2), log_entry(3, 3)]);

    // Leader sends entries conflicting at index 2, with two new entries.
    let leader_id = 0;
    let entries = vec![log_entry(3, 20), log_entry(3, 21)];
    let req = AppendEntriesRequest {
        term: node_term,
        leader_id,
        prev_log_index: 1,
        prev_log_term: 1,
        entries,
        leader_commit: 4,
    };

    let mut out = node
        .handle_message(Message::AppendEntriesRequest(req))
        .unwrap();
    assert_eq!(out.len(), 1);
    let (resp_node_id, msg) = out.remove(0);
    let AppendEntriesResponse {
        term,
        success,
        node_id: out_node_id,
        next_index,
    } = msg.unwrap_append_entries_response();
    assert_eq!(resp_node_id, leader_id);
    assert_eq!(term, node_term);
    assert!(success);
    assert_eq!(out_node_id, node_id);
    assert_eq!(next_index, Some(NonZeroIndex::new(4).unwrap()));

    // Log should be: [(t1, 1), (t3, 20), (t3, 21)].
    assert_eq!(node.log_state.log.last_index(), 3);
    assert_eq!(
        node.log_state.log.inner(),
        &[log_entry(1, 1), log_entry(3, 20), log_entry(3, 21)]
    );

    // Commit index should be min(leader_commit, last_index) = 3, and applied.
    assert_eq!(node.node_state.commit_index, 3);
    assert_eq!(node.node_state.last_applied, 3);
    assert_eq!(node.state_machine.applied, vec![Cmd(1), Cmd(20), Cmd(21)]);
}

// On failure, leader decrements next_index and retries.
#[test]
fn append_entries_response_failure_retries_and_decrements() {
    const N: usize = 3;
    let node_id = 0;
    let node_term = 10;
    let mut node = new_node::<N>(node_id);
    node.log_state.set_term(node_term);

    // Add one entry so prev_log_index lookups are valid.
    let log_term = 9;
    node.log_state.log.push(log_entry(log_term, 1));

    // Become leader manually.
    // Set follower 1 next_index to 3 (so prev_log_index = 2).
    let mut leader_state = LeaderState::new(node.log_state.log.last_index());
    leader_state.next_index[1] = NonZeroIndex::new(3).unwrap();
    node.role = RoleState::Leader { leader_state };

    let follower_id = 1;
    let resp = AppendEntriesResponse {
        term: 10,
        success: false,
        node_id: follower_id,
        next_index: None,
    };

    let mut out = node
        .handle_message(Message::AppendEntriesResponse(resp))
        .unwrap();
    // Should send a new AppendEntriesRequest to follower 1 with prev_log_index now 1.
    assert_eq!(out.len(), 1);
    let (resp_node_id, msg) = out.remove(0);
    let AppendEntriesRequest {
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        entries,
        leader_commit,
    } = msg.unwrap_append_entries_request();
    assert_eq!(resp_node_id, follower_id);
    assert_eq!(term, node_term);
    assert_eq!(leader_id, node_id);
    assert_eq!(prev_log_index, 1);
    assert_eq!(prev_log_term, log_term);
    assert_eq!(entries, Vec::new());
    assert_eq!(leader_commit, node.node_state.commit_index);

    let RoleState::Leader {
        leader_state: LeaderState {
            next_index,
            match_index: _,
        },
    } = node.role
    else {
        panic!("unexpected role: {:?}", node.role);
    };
    assert_eq!(next_index[1], NonZeroIndex::new(2).unwrap());
}

// Failure response does not decrement next_index below 1 but still retries.
#[test]
fn append_entries_response_no_decrement_below_one_but_retries() {
    const N: usize = 3;
    let node_id = 0;
    let node_term = 2;
    let mut node = new_node::<N>(node_id);
    node.log_state.set_term(node_term);

    let log_term = 2;
    node.log_state.log.push(log_entry(log_term, 1));

    // Force follower next_index to 1.
    let mut leader_state = LeaderState::new(node.log_state.log.last_index());
    leader_state.next_index[1] = NonZeroIndex::new(1).unwrap();
    node.role = RoleState::Leader { leader_state };

    let follower_id = 1;
    let resp = AppendEntriesResponse {
        term: 2,
        success: false,
        node_id: follower_id,
        next_index: None,
    };
    // Failure should not decrement below 1 but should still produce a retry request.
    let mut out = node
        .handle_message(Message::AppendEntriesResponse(resp))
        .unwrap();
    assert_eq!(out.len(), 1);
    let (resp_node_id, msg) = out.remove(0);
    let AppendEntriesRequest {
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        entries,
        leader_commit,
    } = msg.unwrap_append_entries_request();
    assert_eq!(resp_node_id, follower_id);
    assert_eq!(term, node_term);
    assert_eq!(leader_id, node_id);
    assert_eq!(prev_log_index, 0);
    assert_eq!(prev_log_term, Term::MIN);
    assert_eq!(entries, node.log_state.log.inner().to_vec());
    assert_eq!(leader_commit, node.node_state.commit_index);

    let RoleState::Leader {
        leader_state: LeaderState {
            next_index,
            match_index: _,
        },
    } = node.role
    else {
        panic!("unexpected role: {:?}", node.role);
    };
    assert_eq!(next_index[1], NonZeroIndex::new(1).unwrap());
}

// Successful AppendEntriesResponse updates indices and can advance commit.
#[test]
fn append_entries_response_success_updates_and_commits() {
    const N: usize = 3;
    let node_id = 0;
    let node_term = 7;
    let mut leader = new_node::<N>(node_id);
    leader.log_state.set_term(node_term);
    leader.role = RoleState::Leader {
        leader_state: LeaderState::new(0),
    };

    // Leader has one entry at term 7.
    leader.log_state.log.push(log_entry(node_term, 42));

    // Followers report success with next_index = 2.
    // First follower updates match_index to 1; not yet majority.
    {
        let resp = AppendEntriesResponse {
            term: node_term,
            success: true,
            node_id: 1,
            next_index: Some(NonZeroIndex::new(2).unwrap()),
        };
        let out = leader
            .handle_message(Message::AppendEntriesResponse(resp))
            .unwrap();
        assert_eq!(out, Vec::new());
    }
    assert_eq!(leader.node_state.commit_index, 0);

    // Second follower success should advance commit to 1.
    {
        let response = AppendEntriesResponse {
            term: 7,
            success: true,
            node_id: 2,
            next_index: Some(NonZeroIndex::new(2).unwrap()),
        };
        let out = leader
            .handle_message(Message::AppendEntriesResponse(response))
            .unwrap();
        assert_eq!(out, Vec::new());
    }
    assert_eq!(leader.node_state.commit_index, 1);
    assert_eq!(leader.node_state.last_applied, 1);
    assert_eq!(leader.state_machine.applied, vec![Cmd(42)]);
}

// Leaders only process AppendEntriesResponse; followers ignore them (both success and failure).
#[test]
fn append_entries_response_ignored_when_not_leader() {
    const N: usize = 3;
    let mut node = new_node::<N>(0);
    node.log_state.set_term(1);
    let role = node.role.clone();
    let log_state = node.log_state.clone();
    let node_state = node.node_state.clone();
    assert!(!role.is_leader());

    // Failure response ignored.
    {
        let resp = AppendEntriesResponse {
            term: 1,
            success: false,
            node_id: 1,
            next_index: None,
        };

        let out = node
            .handle_message(Message::AppendEntriesResponse(resp))
            .unwrap();
        assert_eq!(out, Vec::new());
        assert_eq!(node.role, role);
        assert_eq!(node.log_state, log_state);
        assert_eq!(node.node_state, node_state);
    }

    // Success response ignored.
    {
        let resp = AppendEntriesResponse {
            term: 1,
            success: true,
            node_id: 1,
            next_index: Some(NonZeroIndex::new(1).unwrap()),
        };

        let out = node
            .handle_message(Message::AppendEntriesResponse(resp))
            .unwrap();
        assert_eq!(out, Vec::new());
        assert_eq!(node.role, role);
        assert_eq!(node.log_state, log_state);
        assert_eq!(node.node_state, node_state);
    }
}

// Reject VoteRequest with a lower term.
#[test]
fn vote_request_rejected_lower_term() {
    const N: usize = 3;
    let node_term = 2;
    let mut node = new_node::<N>(3);
    node.log_state.set_term(node_term);

    // Lower term rejected.
    let candidate_id = 1;
    let req = VoteRequest {
        term: 1,
        candidate_id,
        last_log_index: 0,
        last_log_term: 0,
    };

    let mut out = node.handle_message(Message::VoteRequest(req)).unwrap();
    assert_eq!(out.len(), 1);
    let (resp_node_id, msg) = out.remove(0);
    let VoteResponse { term, vote_granted } = msg.unwrap_vote_response();
    assert_eq!(resp_node_id, candidate_id);
    assert_eq!(term, node_term);
    assert!(!vote_granted);
}

// Reject VoteRequest if a node has already voted.
#[test]
fn vote_request_rejected_already_voted() {
    const N: usize = 3;
    let node_term = 3;
    let mut node = new_node::<N>(3);
    node.log_state.set_term(node_term);
    node.log_state.voted_for = Some(0);

    let candidate_id = 2;
    let req = VoteRequest {
        term: node_term,
        candidate_id,
        last_log_index: 0,
        last_log_term: 5,
    };

    let mut out = node.handle_message(Message::VoteRequest(req)).unwrap();
    assert_eq!(out.len(), 1);
    let (resp_node_id, msg) = out.remove(0);
    let VoteResponse { term, vote_granted } = msg.unwrap_vote_response();
    assert_eq!(resp_node_id, candidate_id);
    assert_eq!(term, node_term);
    assert!(!vote_granted);
}

// Reject VoteRequest if a candidate's log has a lower last log term.
#[test]
fn vote_request_rejected_not_up_to_date_by_term() {
    const N: usize = 3;
    let node_term = 10;
    let last_term = 6;
    let mut node = new_node::<N>(3);
    node.log_state.set_term(node_term);
    node.log_state.log.push(log_entry(last_term, 1));

    let candidate_id = 4;
    let req = VoteRequest {
        term: node_term,
        candidate_id,
        last_log_index: 1,
        last_log_term: last_term - 1,
    };

    let mut out = node.handle_message(Message::VoteRequest(req)).unwrap();
    assert_eq!(out.len(), 1);
    let (resp_node_id, msg) = out.remove(0);
    let VoteResponse { term, vote_granted } = msg.unwrap_vote_response();
    assert_eq!(resp_node_id, candidate_id);
    assert_eq!(term, node_term);
    assert!(!vote_granted);
    assert_eq!(node.log_state.voted_for, None);
}

// Reject VoteRequest if a candidate's log has a lower last log index.
#[test]
fn vote_request_rejected_not_up_to_date_by_index() {
    const N: usize = 3;
    let node_term = 10;
    let last_term = 6;
    let mut node = new_node::<N>(3);
    node.log_state.set_term(node_term);
    node.log_state.log.push(log_entry(last_term, 1));

    let candidate_id = 4;
    let req = VoteRequest {
        term: node_term,
        candidate_id,
        last_log_index: 0,
        last_log_term: last_term,
    };

    let mut out = node.handle_message(Message::VoteRequest(req)).unwrap();
    assert_eq!(out.len(), 1);
    let (resp_node_id, msg) = out.remove(0);
    let VoteResponse { term, vote_granted } = msg.unwrap_vote_response();
    assert_eq!(resp_node_id, candidate_id);
    assert_eq!(term, node_term);
    assert!(!vote_granted);
    assert_eq!(node.log_state.voted_for, None);
}

// Grant VoteRequest with higher terms.
#[test]
fn vote_request_granted_higher_term() {
    const N: usize = 3;
    let mut node = new_node::<N>(3);
    node.log_state.set_term(2);

    let candidate_id = 1;
    let vote_term = 3;
    let request = VoteRequest {
        term: vote_term,
        candidate_id,
        last_log_index: 0,
        last_log_term: 5,
    };

    // Grant when up to date by term.
    {
        let mut out = node
            .handle_message(Message::VoteRequest(request.clone()))
            .unwrap();
        assert_eq!(out.len(), 1);
        let (resp_node_id, msg) = out.remove(0);
        let VoteResponse { term, vote_granted } = msg.unwrap_vote_response();
        assert_eq!(resp_node_id, candidate_id);
        assert_eq!(term, vote_term);
        assert!(vote_granted);
    }
    assert_eq!(node.log_state.voted_for, Some(candidate_id));

    // Grant again to the same candidate.
    {
        let mut out = node.handle_message(Message::VoteRequest(request)).unwrap();
        assert_eq!(out.len(), 1);
        let (resp_node_id, msg) = out.remove(0);
        let VoteResponse { term, vote_granted } = msg.unwrap_vote_response();
        assert_eq!(resp_node_id, candidate_id);
        assert_eq!(term, vote_term);
        assert!(vote_granted);
    }
    assert_eq!(node.log_state.voted_for, Some(candidate_id));
}

// Grant VoteRequest if the candidate's log has a larger last log term.
#[test]
fn vote_request_granted_same_term_up_to_date_by_term() {
    const N: usize = 3;
    let node_term = 10;
    let last_term = 6;
    let mut node = new_node::<N>(3);
    node.log_state.set_term(node_term);
    node.log_state.log.push(log_entry(last_term, 1));

    let candidate_id = 4;
    let req = VoteRequest {
        term: node_term,
        candidate_id,
        last_log_index: 1,
        last_log_term: last_term + 1,
    };

    let mut out = node.handle_message(Message::VoteRequest(req)).unwrap();
    assert_eq!(out.len(), 1);
    let (resp_node_id, msg) = out.remove(0);
    let VoteResponse { term, vote_granted } = msg.unwrap_vote_response();
    assert_eq!(resp_node_id, candidate_id);
    assert_eq!(term, node_term);
    assert!(vote_granted);
    assert_eq!(node.log_state.voted_for, Some(candidate_id));
}

// Grant VoteRequest if the candidate's log has a larger last log index.
#[test]
fn vote_request_granted_same_term_up_to_date_by_index() {
    const N: usize = 3;
    let node_term = 10;
    let last_term = 6;
    let mut node = new_node::<N>(3);
    node.log_state.set_term(node_term);
    node.log_state.log.push(log_entry(last_term, 1));

    let candidate_id = 4;
    let req = VoteRequest {
        term: node_term,
        candidate_id,
        last_log_index: 1,
        last_log_term: last_term,
    };

    let mut out = node.handle_message(Message::VoteRequest(req)).unwrap();
    assert_eq!(out.len(), 1);
    let (resp_node_id, msg) = out.remove(0);
    let VoteResponse { term, vote_granted } = msg.unwrap_vote_response();
    assert_eq!(resp_node_id, candidate_id);
    assert_eq!(term, node_term);
    assert!(vote_granted);
    assert_eq!(node.log_state.voted_for, Some(candidate_id));
}

// Rejected VoteResponse is ignored if it's in the same term.
#[test]
fn vote_response_rejected_same_term() {
    const N: usize = 3;
    let node_term = 4;
    let mut node = new_node::<N>(0);
    node.log_state.set_term(node_term);

    let role = RoleState::Candidate { votes: 1 };
    node.role = role.clone();

    let resp = VoteResponse {
        term: node_term,
        vote_granted: false,
    };

    let out = node.handle_message(Message::VoteResponse(resp)).unwrap();
    assert_eq!(out, Vec::new());
    assert_eq!(node.role, role);
}

// Rejected VoteResponse causes a demotion if it's in a later term.
#[test]
fn vote_response_rejected_later_term() {
    const N: usize = 3;
    let node_term = 4;
    let mut node = new_node::<N>(0);
    node.log_state.set_term(node_term);

    let role = RoleState::Candidate { votes: 1 };
    node.role = role.clone();

    let response_term = node_term + 1;
    let resp = VoteResponse {
        term: response_term,
        vote_granted: false,
    };

    let out = node.handle_message(Message::VoteResponse(resp)).unwrap();
    assert_eq!(out, Vec::new());
    assert_eq!(node.role, RoleState::Follower);
    assert_eq!(*node.log_state.current_term(), response_term);
}

// Granted VoteResponse increments vote count.
#[test]
fn vote_response_granted() {
    const N: usize = 3;
    let mut node = new_node::<N>(3);
    node.role = RoleState::Candidate { votes: 0 };

    let resp = VoteResponse {
        term: *node.current_term(),
        vote_granted: true,
    };

    let out = node.handle_message(Message::VoteResponse(resp)).unwrap();
    assert_eq!(out, Vec::new());
    assert_eq!(node.role, RoleState::Candidate { votes: 1 });
}

// A majority of granted VoteResponse promotes the node.
#[test]
fn vote_response_granted_promotion() {
    const N: usize = 3;
    let mut node = new_node::<N>(3);
    node.role = RoleState::Candidate { votes: 1 };
    node.log_state
        .log
        .extend([log_entry(1, 1), log_entry(2, 2)]);

    let resp = VoteResponse {
        term: *node.current_term(),
        vote_granted: true,
    };

    let out = node.handle_message(Message::VoteResponse(resp)).unwrap();
    assert_eq!(out, Vec::new());
    assert_eq!(
        node.role,
        RoleState::Leader {
            leader_state: LeaderState {
                next_index: [NonZeroIndex::new(3).unwrap(); N],
                match_index: [0; N],
            }
        }
    );
}

// VoteResponse is ignored if node is follower or response has an older term.
#[test]
fn vote_response_ignored() {
    const N: usize = 3;
    let node_term = 4;
    let mut node = new_node::<N>(0);
    node.log_state.set_term(node_term);

    // As a follower, ignore.
    {
        assert_eq!(node.role, RoleState::Follower);
        let resp = VoteResponse {
            term: node_term,
            vote_granted: true,
        };

        let out = node.handle_message(Message::VoteResponse(resp)).unwrap();
        assert_eq!(out, Vec::new());
        assert_eq!(node.role.role(), Role::Follower);
    }

    // As a candidate, but older term response ignored.
    {
        let role = RoleState::Candidate { votes: 1 };
        node.role = role.clone();
        let old_term = node_term - 1;
        let resp = VoteResponse {
            term: old_term,
            vote_granted: true,
        };

        let out = node.handle_message(Message::VoteResponse(resp)).unwrap();
        assert_eq!(out, Vec::new());
        assert_eq!(node.role, role);
    }
}

// Becoming a candidate increments term, self-votes, and broadcasts VoteRequest to peers.
#[test]
fn become_candidate_broadcasts_votes() {
    const N: usize = 3;
    let node_id = 1;
    let node_term = 1;
    let mut node = new_node::<N>(node_id);
    node.log_state.set_term(node_term);
    node.log_state.log.push(log_entry(node_term, 0));

    let out = node.handle_message(Message::BecomeCandidate).unwrap();
    // Two peers (0 and 2) receive VoteRequest.
    assert_eq!(out.len(), 2);
    let (response_node_ids, mut msgs): (BTreeSet<_>, Vec<_>) = out.into_iter().unzip();
    assert_eq!(BTreeSet::from([0, 2]), response_node_ids);
    assert_eq!(msgs[0], msgs[1]);

    let VoteRequest {
        term,
        candidate_id,
        last_log_index,
        last_log_term,
    } = msgs.remove(0).unwrap_vote_request();
    assert_eq!(term, node_term + 1);
    assert_eq!(last_log_index, 1);
    assert_eq!(last_log_term, node_term);
    assert_eq!(candidate_id, node_id);
}

// Commands on a follower error with NotLeader, including a leader hint when known.
#[test]
fn command_follower_errors() {
    const N: usize = 3;
    let mut follower = new_node::<N>(0);

    // No known leader yet.
    let cmd = Cmd(1);
    let err = follower
        .handle_message(Message::Command(cmd.clone()))
        .unwrap_err();
    assert_eq!(err, Error::NotLeader(None));

    // Leader known.
    follower.node_state.leader_id = Some(2);
    let err = follower.handle_message(Message::Command(cmd)).unwrap_err();
    assert_eq!(err, Error::NotLeader(Some(2)));
}

// Accepted client command sends AppendEntries to peers.
#[test]
fn command_leader_sends_append_entries() {
    const N: usize = 4;
    let node_id = 1;
    let node_term = 3;
    let mut entries = vec![log_entry(1, 1), log_entry(2, 2), log_entry(3, 3)];
    let leader_state = LeaderState {
        next_index: [
            NonZeroIndex::new(2).unwrap(),
            NonZeroIndex::new(4).unwrap(),
            NonZeroIndex::new(4).unwrap(),
            NonZeroIndex::new(1).unwrap(),
        ],
        match_index: [0; N],
    };
    let mut node = new_node::<N>(node_id);
    node.log_state.set_term(node_term);
    node.log_state.log.extend(entries.clone());
    node.role = RoleState::Leader { leader_state };

    let cmd = Cmd(99);
    let new_log_entry = log_entry(node_term, cmd.0);
    entries.push(new_log_entry.clone());

    let mut out = node.handle_message(Message::Command(cmd)).unwrap();
    out.sort_by_key(|(node_id, _)| *node_id);
    // Three append requests to peers.
    assert_eq!(out.len(), 3);
    let (response_node_ids, msgs): (Vec<_>, Vec<_>) = out.into_iter().unzip();
    assert_eq!(response_node_ids, vec![0, 2, 3]);
    let append_entries_requests: Vec<_> = msgs
        .into_iter()
        .map(|msg| msg.unwrap_append_entries_request())
        .collect();
    let expected_append_entries_requests = vec![
        AppendEntriesRequest {
            term: node_term,
            leader_id: node_id,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: entries[1..].to_vec(),
            leader_commit: node.node_state.commit_index,
        },
        AppendEntriesRequest {
            term: node_term,
            leader_id: node_id,
            prev_log_index: 3,
            prev_log_term: 3,
            entries: vec![new_log_entry],
            leader_commit: node.node_state.commit_index,
        },
        AppendEntriesRequest {
            term: node_term,
            leader_id: node_id,
            prev_log_index: 0,
            prev_log_term: Term::MIN,
            entries,
            leader_commit: node.node_state.commit_index,
        },
    ];
    assert_eq!(append_entries_requests, expected_append_entries_requests);
}

// Any message with a higher term resets term, clears voted_for, and steps down.
#[test]
fn higher_term_resets_and_clears_vote() {
    const N: usize = 3;
    let mut node = new_node::<N>(1);
    let _ = node.handle_message(Message::BecomeCandidate).unwrap();
    node.log_state.voted_for = Some(42);

    // Receive higher term AppendEntriesResponse (could be any).
    let _ = node
        .handle_message(Message::AppendEntriesResponse(AppendEntriesResponse {
            term: *node.current_term() + 5,
            success: false,
            node_id: 0,
            next_index: None,
        }))
        .unwrap();
    assert_eq!(node.role.role(), Role::Follower);
    assert_eq!(node.log_state.voted_for, None);
}

// Majority replication does not commit entries from a prior term.
#[test]
fn no_commit_prev_term() {
    const N: usize = 3;
    let mut leader = new_node::<N>(0);
    // Current term 7, but the log entry is from term 6.
    leader.log_state.set_term(7);
    leader.role = RoleState::Leader {
        leader_state: LeaderState::new(0),
    };
    // index 1, term 6.
    leader.log_state.log.push(log_entry(6, 5));

    let resp1 = AppendEntriesResponse {
        term: 7,
        success: true,
        node_id: 1,
        next_index: Some(NonZeroIndex::new(2).unwrap()),
    };
    let resp2 = AppendEntriesResponse {
        term: 7,
        success: true,
        node_id: 2,
        next_index: Some(NonZeroIndex::new(2).unwrap()),
    };
    let out = leader
        .handle_message(Message::AppendEntriesResponse(resp1))
        .unwrap();
    assert_eq!(out, Vec::new());
    let out = leader
        .handle_message(Message::AppendEntriesResponse(resp2))
        .unwrap();
    assert_eq!(out, Vec::new());

    // Commit index should not advance because log[1].term != currentTerm.
    assert_eq!(leader.node_state.commit_index, 0);
    assert_eq!(leader.node_state.last_applied, 0);
    assert_eq!(leader.state_machine.applied, Vec::new());
}

#[cfg(test)]
mod prop_sm {
    //! Property-based state-machine tests (single-node).

    use proptest::prelude::*;
    use proptest::test_runner::TestCaseResult;

    use super::*;

    /// A minimal model to carry expectations across steps (single-node).
    #[derive(Clone, Debug, Default)]
    struct Model {
        last_term: Term,
        last_commit_index: Index,
        last_applied_index: Index,
    }

    // System Under Test wrapper.
    struct Sut<const N: usize> {
        node: RaftCore<N, Cmd, TestStateMachine, TestStorage>,
    }

    impl<const N: usize> Sut<N> {
        fn new(node_id: NodeId) -> Self {
            Self {
                node: new_node::<N>(node_id),
            }
        }

        fn check_invariants(&self, model: &Model) -> TestCaseResult {
            // Term monotonicity.
            prop_assert!(
                *self.node.current_term() >= model.last_term,
                "current term, {}, decreased from last term, {}",
                self.node.current_term(),
                model.last_term,
            );
            // Commit monotonicity.
            prop_assert!(
                self.node.node_state.commit_index >= model.last_commit_index,
                "current commit index, {}, decreased from last commit index, {}",
                self.node.node_state.commit_index,
                model.last_commit_index,
            );
            // Applied monotonicity.
            prop_assert!(
                self.node.node_state.last_applied >= model.last_applied_index,
                "current applied index, {}, decreased from last applied index, {}",
                self.node.node_state.last_applied,
                model.last_applied_index,
            );
            // Commit index never exceeds last log index.
            prop_assert!(
                self.node.node_state.commit_index <= self.node.log_state.log.last_index(),
                "commit index, {}, exceeds last log index, {}",
                self.node.node_state.commit_index,
                self.node.log_state.log.last_index(),
            );
            // Applied never exceeds the commit index.
            prop_assert!(
                self.node.node_state.last_applied <= self.node.node_state.commit_index,
                "applied index, {}, exceeds commit index, {}",
                self.node.node_state.last_applied,
                self.node.node_state.commit_index,
            );
            // Log terms are never larger than the current term.
            prop_assert!(
                self.node
                    .log_state
                    .log
                    .inner()
                    .iter()
                    .all(|log_entry| log_entry.term <= *self.node.current_term()),
                "log contains larger term than current term, {}; {:?}",
                self.node.current_term(),
                self.node.log_state.log,
            );
            Ok(())
        }
    }

    proptest! {
        // Use a small number of steps to keep test time reasonable in CI.
        #[test]
        fn raft_single_node_state_machine(ops in prop::collection::vec(any::<Message<Cmd>>(), 256)) {
            let mut sut = Sut::<3>::new(1);
            let mut model = Model::default();
            for msg in ops.into_iter() {
                // Execute operation on SUT. We ignore returned outgoing messages; we only assert invariants.
                let (expect_ok, apply) = match msg {
                    Message::AppendEntriesRequest(_)
                    | Message::AppendEntriesResponse(_)
                    | Message::VoteRequest(_)
                    | Message::VoteResponse(_) => (true, true),
                    Message::Command(_) => (false, true),
                    Message::BecomeCandidate => (true, sut.node.role.role() == Role::Follower),
                };

                if apply {
                    let res = sut.node.handle_message(msg);
                    if expect_ok {
                        prop_assert!(res.is_ok());
                    }
                }

                // Check invariants after each step.
                sut.check_invariants(&model)?;

                // Update model.
                model.last_term = *sut.node.current_term();
                model.last_commit_index = sut.node.node_state.commit_index;
                model.last_applied_index = sut.node.node_state.last_applied;
            }
        }
    }
}
