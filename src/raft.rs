//! This module contains the [Raft](https://raft.github.io/raft.pdf) consensus algorithm.

#[cfg(test)]
mod prop_tests;
#[cfg(test)]
mod tests;

use derivative::Derivative;

use crate::error::Error;
use crate::message::{
    AppendEntriesRequest, AppendEntriesResponse, Message, VoteRequest, VoteResponse,
};
use crate::state::{
    Command, LeaderState, LogEntry, LogState, NodeId, NodeState, NonZeroIndex, Role, RoleState,
    Term,
};
use crate::storage::Storage;

pub trait StateMachine<C: Command> {
    fn apply(&mut self, entry: &C);
}

/// The deterministic parts of the Raft algorithm for a single node. Specifically, this does not
/// contain any timers or networking code.
#[derive(Derivative, Clone)]
#[derivative(Debug, PartialEq, Eq)]
struct RaftCore<const N: usize, C: Command, SM: StateMachine<C>, S: Storage<C>> {
    node_id: NodeId,
    log_state: LogState<C>,
    node_state: NodeState,
    role: RoleState<N>,
    #[derivative(Debug = "ignore", PartialEq = "ignore")]
    state_machine: SM,
    #[derivative(Debug = "ignore", PartialEq = "ignore")]
    storage: S,
}

#[cfg_attr(not(test), expect(dead_code))]
impl<const N: usize, C: Command, SM: StateMachine<C>, S: Storage<C>> RaftCore<N, C, SM, S> {
    fn new(node_id: NodeId, state_machine: SM, storage: S) -> Self {
        Self {
            node_id,
            log_state: LogState::new(&storage),
            node_state: NodeState::new(),
            role: RoleState::Follower,
            state_machine,
            storage,
        }
    }

    fn handle_message(&mut self, msg: Message<C>) -> Result<Vec<(NodeId, Message<C>)>, Error> {
        match msg {
            Message::AppendEntriesRequest(append_entries_request) => {
                let node_id = append_entries_request.leader_id;
                let append_entries_response = self.append_entries_request(append_entries_request);
                Ok(vec![(
                    node_id,
                    Message::AppendEntriesResponse(append_entries_response),
                )])
            }
            Message::AppendEntriesResponse(append_entries_response) => {
                let node_id = append_entries_response.node_id;
                let append_entries_request = self.append_entries_response(append_entries_response);
                Ok(append_entries_request
                    .into_iter()
                    .map(|append_entries_request| {
                        (
                            node_id,
                            Message::AppendEntriesRequest(append_entries_request),
                        )
                    })
                    .collect())
            }
            Message::VoteRequest(vote_request) => {
                let node_id = vote_request.candidate_id;
                let vote_response = self.vote_request(vote_request);
                Ok(vec![(node_id, Message::VoteResponse(vote_response))])
            }
            Message::VoteResponse(vote_response) => {
                self.vote_response(vote_response);
                Ok(Vec::new())
            }
            Message::BecomeCandidate => {
                let vote_request = self.become_candidate();
                // TODO: Cloning the same message is a bit wasteful.
                Ok((0..N)
                    .map(|node_id| node_id as NodeId)
                    .filter(|node_id| *node_id != self.node_id)
                    .map(|peer| (peer, Message::VoteRequest(vote_request.clone())))
                    .collect())
            }
            Message::Command(command) => match self.command(command) {
                Ok(append_entries_requests) => Ok(append_entries_requests
                    .into_iter()
                    .map(|(node_id, append_entries_request)| {
                        (
                            node_id,
                            Message::AppendEntriesRequest(append_entries_request),
                        )
                    })
                    .collect()),
                Err(leader_id) => Err(Error::NotLeader(leader_id)),
            },
        }
    }

    /// Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
    fn append_entries_request(
        &mut self,
        AppendEntriesRequest {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            mut entries,
            leader_commit,
        }: AppendEntriesRequest<C>,
    ) -> AppendEntriesResponse {
        // If AppendEntries RPC received from new leader: convert to
        // follower.
        if term == *self.current_term() && self.role.is_candidate() {
            self.role = RoleState::Follower;
        }
        self.process_term(term);

        let failed_response = || AppendEntriesResponse {
            term: *self.current_term(),
            success: false,
            node_id: self.node_id,
            next_index: None,
        };

        // Reply false if term < currentTerm (§5.1).
        if term < *self.current_term() {
            return failed_response();
        }

        // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches
        // prevLogTerm (§5.3).
        //
        // Note: prev_log_index == 0 is allowed.
        if prev_log_index != 0 {
            let Some(prev_log) = self.log_state.log().get(prev_log_index) else {
                return failed_response();
            };
            if prev_log.term != prev_log_term {
                return failed_response();
            }
        }

        // If AppendEntries RPC received from new leader: convert to
        // follower.
        self.role = RoleState::Follower;

        // If an existing entry conflicts with a new one (same index but different terms), delete
        // the existing entry and all that follow it (§5.3).
        let next_index = NonZeroIndex::new(prev_log_index + 1)
            .expect("adding one ensures that this is not zero");
        if let Some(conflicting_idx) = entries
            .iter()
            .zip(self.log_state.log()[next_index..].iter())
            .position(|(new_entry, existing_entry)| new_entry.term != existing_entry.term)
        {
            let truncate_index = next_index
                .checked_add(conflicting_idx as u64)
                .expect("log index overflow")
                .into();
            self.truncate_log(truncate_index);
        }

        // Append any new entries not already in the log.
        let new_entries_idx = (self.log_state.log().next_index().get() - next_index.get()) as usize;
        entries.drain(0..new_entries_idx);
        self.extend_log(entries);

        self.node_state.leader_id = Some(leader_id);

        // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new
        // entry).
        if leader_commit > self.node_state.commit_index {
            self.node_state.commit_index =
                std::cmp::min(leader_commit, self.log_state.log().last_index());
        }

        self.apply_committed_entries();

        AppendEntriesResponse {
            term: *self.current_term(),
            success: true,
            node_id: self.node_id,
            next_index: Some(self.log_state.log().next_index()),
        }
    }

    fn append_entries_response(
        &mut self,
        AppendEntriesResponse {
            term,
            success,
            node_id,
            next_index,
        }: AppendEntriesResponse,
    ) -> Option<AppendEntriesRequest<C>> {
        self.process_term(term);

        if success {
            let next_index = next_index.expect("success must have a next index");
            self.append_entries_response_success(node_id, next_index);
            None
        } else {
            self.append_entries_response_failure(node_id)
        }
    }

    fn append_entries_response_success(
        &mut self,
        node_id: NodeId,
        follower_next_index: NonZeroIndex,
    ) {
        let current_term = *self.current_term();
        let RoleState::Leader {
            leader_state:
                LeaderState {
                    next_index,
                    match_index,
                },
        } = &mut self.role
        else {
            return;
        };

        // If successful: update nextIndex and matchIndex for follower (§5.3).
        let node_idx = node_id as usize;
        next_index[node_idx] = std::cmp::max(follower_next_index, next_index[node_idx]);
        match_index[node_idx] = std::cmp::max(follower_next_index.get() - 1, match_index[node_idx]);

        // If there exists an N such that N > commitIndex, a majority
        // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
        // set commitIndex = N (§5.3, §5.4).
        let mut match_index: Vec<_> = match_index.iter().copied().collect();
        match_index.sort_unstable();
        let new_commit_index = match_index[match_index.len() / 2];
        if let Some(new_commit_entry) = self.log_state.log().get(new_commit_index)
            && new_commit_entry.term == current_term
        {
            self.node_state.commit_index =
                std::cmp::max(new_commit_index, self.node_state.commit_index);
        }

        self.apply_committed_entries();
    }

    fn append_entries_response_failure(
        &mut self,
        node_id: NodeId,
    ) -> Option<AppendEntriesRequest<C>> {
        // If AppendEntries fails because of log inconsistency:
        // decrement nextIndex and retry (§5.3).
        //
        // Note: It's possible that this is an old failure that will cause us to unnecessarily
        // decrement `next_index`. This is fine because we'll just send an already replicated
        // entry which is handled by the follower. Failures should be rare in practice, so this
        // shouldn't be a huge issue.
        let RoleState::Leader {
            leader_state:
                LeaderState {
                    next_index,
                    match_index: _,
                },
        } = &mut self.role
        else {
            return None;
        };

        let next_index = next_index
            .get_mut(node_id as usize)
            .expect("node must exist");
        if let Some(new_next_index) = NonZeroIndex::new(next_index.get() - 1) {
            *next_index = new_next_index;
        }
        Some(self.generate_append_entry_request_for(node_id))
    }

    /// Invoked by candidates to gather votes (§5.2).
    fn vote_request(
        &mut self,
        VoteRequest {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }: VoteRequest,
    ) -> VoteResponse {
        self.process_term(term);

        // Reply false if term < currentTerm (§5.1).
        if term < *self.current_term() {
            return VoteResponse {
                term: *self.current_term(),
                vote_granted: false,
            };
        }

        // If votedFor is null or candidateId, and candidate's log is at
        // least as up-to-date as receiver's log, grant vote (§5.2, §5.4).
        //
        // Raft determines which of two logs is more up-to-date
        // by comparing the index and term of the last entries in the
        // logs. If the logs have last entries with different terms, then
        // the log with the later term is more up-to-date. If the logs
        // end with the same term, then whichever log is longer is
        // more up-to-date.
        if self.log_state.voted_for().is_none() || *self.log_state.voted_for() == Some(candidate_id)
        {
            let my_last_index = self.log_state.log().last_index();
            let my_last_term = self
                .log_state
                .log()
                .last()
                .map(|entry| entry.term)
                .unwrap_or(Term::MIN);

            if last_log_term > my_last_term
                || (last_log_term == my_last_term && last_log_index >= my_last_index)
            {
                self.set_voted_for(candidate_id);
                return VoteResponse {
                    term: *self.current_term(),
                    vote_granted: true,
                };
            }
        }

        VoteResponse {
            term: *self.current_term(),
            vote_granted: false,
        }
    }

    fn vote_response(&mut self, VoteResponse { term, vote_granted }: VoteResponse) {
        let current_term = *self.current_term();
        self.process_term(term);

        // Old election response so we need to ignore it.
        if term < current_term {
            return;
        }

        let RoleState::Candidate { votes } = &mut self.role else {
            return;
        };
        if vote_granted {
            *votes += 1;
            // If votes received from majority of servers: become leader.
            if *votes > N / 2 {
                self.role = RoleState::Leader {
                    leader_state: LeaderState::new(self.log_state.log().last_index()),
                };
            }
        }
    }

    fn become_candidate(&mut self) -> VoteRequest {
        assert_eq!(self.role.role(), Role::Follower);
        // Increment currentTerm.
        self.increment_term();
        self.role = RoleState::Candidate { votes: 0 };

        // Vote for self.
        self.set_voted_for(self.node_id);
        self.vote_response(VoteResponse {
            term: *self.current_term(),
            vote_granted: true,
        });

        VoteRequest {
            term: *self.current_term(),
            candidate_id: self.node_id,
            last_log_index: self.log_state.log().last_index(),
            last_log_term: self
                .log_state
                .log()
                .last()
                .map(|entry| entry.term)
                .unwrap_or(Term::MIN),
        }
    }

    fn command(
        &mut self,
        command: C,
    ) -> Result<Vec<(NodeId, AppendEntriesRequest<C>)>, Option<NodeId>> {
        if !self.role.is_leader() {
            return Err(self.node_state.leader_id);
        }

        self.push_log(LogEntry {
            term: *self.current_term(),
            command,
        });

        let mut append_entry_requests = Vec::with_capacity(N - 1);
        for node_id in 0..N {
            let node_id = node_id as NodeId;
            if node_id == self.node_id {
                continue;
            }
            let append_entry_request = self.generate_append_entry_request_for(node_id);
            append_entry_requests.push((node_id, append_entry_request));
        }

        // Update state for self.
        let result = self.append_entries_response(AppendEntriesResponse {
            term: *self.current_term(),
            success: true,
            node_id: self.node_id,
            next_index: Some(self.log_state.log().next_index()),
        });
        assert_eq!(
            result, None,
            "updating self should never need to be retried"
        );

        Ok(append_entry_requests)
    }

    /// Generate [`AppendEntriesRequest`] for node `node_id`.
    ///
    /// Panics if not leader.
    fn generate_append_entry_request_for(&self, node_id: NodeId) -> AppendEntriesRequest<C> {
        let RoleState::Leader {
            leader_state:
                LeaderState {
                    next_index,
                    match_index: _,
                },
        } = &self.role
        else {
            panic!("not leader: {:?}", self.role);
        };

        let next_index = next_index[node_id as usize];
        let prev_log_index = next_index.get() - 1;
        let prev_log_term = self
            .log_state
            .log()
            .get(prev_log_index)
            .map(|entry| entry.term)
            .unwrap_or(Term::MIN);
        // TODO: Try and avoid these clones.
        let entries = self.log_state.log()[next_index..].to_vec();
        AppendEntriesRequest {
            term: *self.current_term(),
            leader_id: self.node_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.node_state.commit_index,
        }
    }

    /// If commitIndex > lastApplied: increment lastApplied, apply
    /// log[lastApplied] to state machine (§5.3).
    fn apply_committed_entries(&mut self) {
        while self.node_state.commit_index > self.node_state.last_applied {
            let entry = self
                .log_state
                .log()
                .get(self.node_state.last_applied + 1)
                .expect("cannot commit a non-existent entry");
            self.state_machine.apply(&entry.command);
            self.node_state.last_applied += 1;
        }
    }

    /// If RPC request or response contains term T > currentTerm:
    /// set currentTerm = T, convert to follower (§5.1).
    fn process_term(&mut self, term: Term) {
        if term > *self.current_term() {
            self.set_term(term);
            self.role = RoleState::Follower;
        }
    }

    fn current_term(&self) -> &Term {
        self.log_state.current_term()
    }

    fn set_term(&mut self, term: Term) {
        self.log_state.set_term(term, &self.storage);
    }

    fn increment_term(&mut self) {
        self.log_state.increment_term(&self.storage);
    }

    fn set_voted_for(&mut self, node_id: NodeId) {
        self.log_state.set_voted_for(node_id, &self.storage);
    }

    fn truncate_log(&mut self, start: NonZeroIndex) {
        assert!(
            self.node_state.commit_index < start.get(),
            "cannot truncate committed entries; commit index: {}, truncate index: {}",
            self.node_state.commit_index,
            start.get()
        );
        self.log_state.truncate_log(start, &self.storage);
    }

    fn extend_log(&mut self, entries: impl IntoIterator<Item = LogEntry<C>>) {
        self.log_state.extend_log(entries, &self.storage);
    }

    fn push_log(&mut self, entry: LogEntry<C>) {
        self.log_state.push_log(entry, &self.storage);
    }
}
