use proptest::prelude::{Arbitrary, BoxedStrategy, Just, Strategy, any, prop, prop_oneof};
use proptest_derive::Arbitrary;

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
    BecomeCandidate,
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

#[derive(Debug, Clone, Eq, PartialEq, Arbitrary)]
pub(crate) struct VoteResponse {
    pub(crate) term: Term,
    pub(crate) vote_granted: bool,
}

impl<C: Command + Arbitrary + 'static> Arbitrary for Message<C>
where
    <C as Arbitrary>::Strategy: 'static,
{
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            any::<AppendEntriesRequest<C>>().prop_map(|append_entries_request| {
                Message::AppendEntriesRequest(append_entries_request)
            }),
            any::<AppendEntriesResponse>().prop_map(|append_entries_response| {
                Message::AppendEntriesResponse(append_entries_response)
            }),
            any::<VoteRequest>().prop_map(|vote_request| Message::VoteRequest(vote_request)),
            any::<VoteResponse>().prop_map(|vote_response| Message::VoteResponse(vote_response)),
            Just(Message::BecomeCandidate),
            any::<C>().prop_map(|cmd| Message::Command(cmd))
        ]
        .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

impl<C: Command + Arbitrary> Arbitrary for AppendEntriesRequest<C>
where
    <C as Arbitrary>::Strategy: 'static,
{
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (any::<Term>(), any::<NodeId>(), any::<Index>())
            .prop_flat_map(|(term, leader_id, prev_log_index)| {
                let prev_log_term = 0..=term;
                // Limit entries to 5 to avoid proptest timeouts.
                let entries = prop::collection::vec(LogEntry::<C>::arbitrary_with(term), 0..5);
                (
                    Just(term),
                    Just(leader_id),
                    Just(prev_log_index),
                    prev_log_term,
                    entries,
                )
            })
            .prop_flat_map(
                |(term, leader_id, prev_log_index, prev_log_term, entries)| {
                    let max_commit_index = prev_log_index + entries.len() as Index;
                    (
                        Just(term),
                        Just(leader_id),
                        Just(prev_log_index),
                        Just(prev_log_term),
                        Just(entries),
                        0..=max_commit_index,
                    )
                },
            )
            .prop_map(
                |(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit)| {
                    AppendEntriesRequest {
                        term,
                        leader_id,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit,
                    }
                },
            )
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

impl Arbitrary for AppendEntriesResponse {
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<Term>(),
            any::<bool>(),
            any::<NodeId>(),
            any::<NonZeroIndex>(),
        )
            .prop_flat_map(|(term, success, node_id, next_index)| {
                let next_index = if success {
                    Just(Some(next_index))
                } else {
                    Just(None)
                };
                (Just(term), Just(success), Just(node_id), next_index)
            })
            .prop_map(
                |(term, success, node_id, next_index)| AppendEntriesResponse {
                    term,
                    success,
                    node_id,
                    next_index,
                },
            )
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

impl Arbitrary for VoteRequest {
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (any::<Term>(), any::<NodeId>(), any::<Index>())
            .prop_flat_map(|(term, candidate_id, last_log_index)| {
                let last_log_term = (0..=term).boxed();
                (
                    Just(term),
                    Just(candidate_id),
                    Just(last_log_index),
                    last_log_term,
                )
            })
            .prop_map(
                |(term, candidate_id, last_log_index, last_log_term)| VoteRequest {
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                },
            )
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}
