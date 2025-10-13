use crate::state::{Command, Index, LogEntry, NodeId, NonZeroIndex, Term};

#[cfg(test)]
macro_rules! unwrap_message {
    ($value:expr, $enum:ident :: $variant:ident) => {
        match $value {
            $enum::$variant(msg) => msg,
            _ => panic!("unexpected message: {:?}", $value),
        }
    };
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum Message<C: Command> {
    AppendEntriesRequest(AppendEntriesRequest<C>),
    AppendEntriesResponse(AppendEntriesResponse),
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    #[cfg_attr(not(test), expect(dead_code))]
    BecomeCandidate,
    #[cfg_attr(not(test), expect(dead_code))]
    Command(C),
}

impl<C: Command> Message<C> {
    #[cfg(test)]
    pub(crate) fn unwrap_append_entries_request(self) -> AppendEntriesRequest<C> {
        unwrap_message!(self, Message::AppendEntriesRequest)
    }

    #[cfg(test)]
    pub(crate) fn unwrap_append_entries_response(self) -> AppendEntriesResponse {
        unwrap_message!(self, Message::AppendEntriesResponse)
    }

    #[cfg(test)]
    pub(crate) fn unwrap_vote_request(self) -> VoteRequest {
        unwrap_message!(self, Message::VoteRequest)
    }

    #[cfg(test)]
    pub(crate) fn unwrap_vote_response(self) -> VoteResponse {
        unwrap_message!(self, Message::VoteResponse)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct AppendEntriesRequest<C: Command> {
    pub(crate) term: Term,
    pub(crate) leader_id: NodeId,
    pub(crate) prev_log_index: Index,
    pub(crate) prev_log_term: Term,
    pub(crate) entries: Vec<LogEntry<C>>,
    pub(crate) leader_commit: Index,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct AppendEntriesResponse {
    pub(crate) term: Term,
    pub(crate) success: bool,
    pub(crate) node_id: NodeId,
    pub(crate) next_index: Option<NonZeroIndex>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct VoteRequest {
    pub(crate) term: Term,
    pub(crate) candidate_id: NodeId,
    pub(crate) last_log_index: Index,
    pub(crate) last_log_term: Term,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct VoteResponse {
    pub(crate) term: Term,
    pub(crate) vote_granted: bool,
}
