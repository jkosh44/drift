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

/// The set of all messages that the Raft state machine can receive from another node.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum InterNodeMessage<C: Command> {
    AppendEntriesRequest(AppendEntriesRequest<C>),
    AppendEntriesResponse(AppendEntriesResponse),
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
}

impl<C: Command> InterNodeMessage<C> {
    /// Returns the term of this message.
    pub fn term(&self) -> Term {
        match self {
            InterNodeMessage::AppendEntriesRequest(msg) => msg.term,
            InterNodeMessage::AppendEntriesResponse(msg) => msg.term,
            InterNodeMessage::VoteRequest(msg) => msg.term,
            InterNodeMessage::VoteResponse(msg) => msg.term,
        }
    }

    #[cfg(test)]
    pub(crate) fn unwrap_append_entries_request(self) -> AppendEntriesRequest<C> {
        unwrap_message!(self, InterNodeMessage::AppendEntriesRequest)
    }

    #[cfg(test)]
    pub(crate) fn unwrap_append_entries_response(self) -> AppendEntriesResponse {
        unwrap_message!(self, InterNodeMessage::AppendEntriesResponse)
    }

    #[cfg(test)]
    pub(crate) fn unwrap_vote_request(self) -> VoteRequest {
        unwrap_message!(self, InterNodeMessage::VoteRequest)
    }

    #[cfg(test)]
    pub(crate) fn unwrap_vote_response(self) -> VoteResponse {
        unwrap_message!(self, InterNodeMessage::VoteResponse)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AppendEntriesRequest<C: Command> {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: Index,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry<C>>,
    pub leader_commit: Index,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
    pub node_id: NodeId,
    pub next_index: Option<NonZeroIndex>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: Index,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}
