//! Property-based state-machine tests (single-node).

use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

use super::*;
use crate::raft::tests::{Cmd, TestStateMachine, TestStorage};
use crate::state::{Index, Log};
use crate::storage::Storage;

/// A minimal model to carry expectations across steps (single-node).
#[derive(Clone, Debug)]
struct Model {
    last_term: Term,
    last_commit_index: Index,
    last_applied_index: Index,
}

impl RaftCore<Cmd, TestStateMachine, TestStorage> {
    fn model(&self) -> Model {
        Model {
            last_term: *self.log_state.current_term(),
            last_commit_index: self.node_state.commit_index,
            last_applied_index: self.node_state.last_applied,
        }
    }
}

fn check_invariants(
    node: &RaftCore<Cmd, TestStateMachine, TestStorage>,
    model: &Model,
) -> TestCaseResult {
    // Term monotonicity.
    prop_assert!(
        *node.current_term() >= model.last_term,
        "current term, {}, decreased from last term, {}",
        node.current_term(),
        model.last_term,
    );
    // Commit monotonicity.
    prop_assert!(
        node.node_state.commit_index >= model.last_commit_index,
        "current commit index, {}, decreased from last commit index, {}",
        node.node_state.commit_index,
        model.last_commit_index,
    );
    // Applied monotonicity.
    prop_assert!(
        node.node_state.last_applied >= model.last_applied_index,
        "current applied index, {}, decreased from last applied index, {}",
        node.node_state.last_applied,
        model.last_applied_index,
    );
    // Commit index never exceeds last log index.
    prop_assert!(
        node.node_state.commit_index <= node.log_state.log().last_index(),
        "commit index, {}, exceeds last log index, {}",
        node.node_state.commit_index,
        node.log_state.log().last_index(),
    );
    // Applied never exceeds the commit index.
    prop_assert!(
        node.node_state.last_applied <= node.node_state.commit_index,
        "applied index, {}, exceeds commit index, {}",
        node.node_state.last_applied,
        node.node_state.commit_index,
    );
    // Log terms are never larger than the current term.
    prop_assert!(
        node.log_state
            .log()
            .inner()
            .iter()
            .all(|log_entry| log_entry.term <= *node.current_term()),
        "log contains larger term than current term, {}; {:?}",
        node.current_term(),
        node.log_state.log(),
    );
    // Log sorted by term.
    prop_assert!(
        node.log_state
            .log()
            .inner()
            .is_sorted_by_key(|log_entry| log_entry.term),
        "log should be sorted by term: {:?}",
        node.log_state.log()
    );
    Ok(())
}

#[derive(Debug, Clone)]
struct TestCase {
    node: RaftCore<Cmd, TestStateMachine, TestStorage>,
    messages: Vec<Message<Cmd>>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum Message<C: Command> {
    InterNodeMessage(InterNodeMessage<C>),
    Command(C),
    BecomeCandidate,
}

impl Arbitrary for TestCase {
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        any::<RaftCore<Cmd, TestStateMachine, TestStorage>>()
            .prop_flat_map(|node| {
                let last_index = node.log_state.log().last_index();
                let message = any_with::<Message<Cmd>>((
                    node.cluster_size,
                    last_index,
                    *node.log_state.current_term(),
                ));
                let messages = prop::collection::vec(message, 64);

                (Just(node), messages)
            })
            .prop_map(|(node, messages)| TestCase { node, messages })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

impl<C, SM, S> Arbitrary for RaftCore<C, SM, S>
where
    C: Command + Arbitrary + 'static,
    <C as Arbitrary>::Strategy: 'static,
    SM: StateMachine<C> + Default,
    S: Storage<C> + Default,
{
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        // Limit cluster sizes to 15
        (1_usize..15)
            .prop_flat_map(|cluster_size| {
                (
                    Just(cluster_size),
                    0..cluster_size,
                    any_with::<LogState<C>>(cluster_size),
                )
            })
            .prop_flat_map(|(cluster_size, node_id, log_state)| {
                let node_state =
                    any_with::<NodeState>((cluster_size, log_state.log().last_index()));
                let role = any_with::<RoleState>((cluster_size, log_state.log().last_index()));
                (
                    Just(cluster_size),
                    Just(node_id as NodeId),
                    Just(log_state),
                    node_state,
                    role,
                )
            })
            .prop_map(
                |(cluster_size, node_id, log_state, node_state, role)| RaftCore {
                    node_id,
                    cluster_size,
                    log_state,
                    node_state,
                    role,
                    state_machine: SM::default(),
                    storage: S::default(),
                },
            )
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

impl<C: Command + Arbitrary + 'static> Arbitrary for LogState<C> {
    type Parameters = usize;

    fn arbitrary_with(num_nodes: Self::Parameters) -> Self::Strategy {
        (any::<Term>(), 0..num_nodes, any::<bool>())
            .prop_flat_map(|(current_term, voted_for, present)| {
                let voted_for = if present {
                    Some(voted_for as NodeId)
                } else {
                    None
                };
                let log = any_with::<Log<C>>(current_term);
                (Just(current_term), Just(voted_for), log)
            })
            .prop_map(|(current_term, voted_for, log)| {
                LogState::from_parts(current_term, voted_for, log)
            })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

impl<C: Command + Arbitrary + 'static> Arbitrary for Log<C> {
    type Parameters = Term;

    fn arbitrary_with(max_term: Self::Parameters) -> Self::Strategy {
        prop::collection::vec(any_with::<LogEntry<C>>(max_term), 0..256)
            .prop_map(|mut log| {
                // Log must be sorted by term.
                log.sort_by_key(|log_entry| log_entry.term);
                Log::from_inner(log)
            })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

impl<C: Command + Arbitrary> Arbitrary for LogEntry<C>
where
    <C as Arbitrary>::Strategy: 'static,
{
    type Parameters = Term;

    fn arbitrary_with(max_term: Self::Parameters) -> Self::Strategy {
        (0..=max_term, any::<C>())
            .prop_map(|(term, command)| LogEntry { term, command })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

impl Arbitrary for NodeState {
    type Parameters = (usize, Index);

    fn arbitrary_with((num_nodes, last_index): Self::Parameters) -> Self::Strategy {
        (0..=last_index, 0..num_nodes, any::<bool>())
            .prop_flat_map(|(commit_index, leader_id, present)| {
                let leader_id = if present {
                    Some(leader_id as NodeId)
                } else {
                    None
                };
                (Just(commit_index), 0..=commit_index, Just(leader_id))
            })
            .prop_map(|(commit_index, last_applied, leader_id)| NodeState {
                commit_index,
                last_applied,
                leader_id,
            })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

impl Arbitrary for RoleState {
    type Parameters = (usize, Index);

    fn arbitrary_with((cluster_size, last_index): Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Just(RoleState::Follower),
            (0..=(cluster_size / 2)).prop_map(|votes| RoleState::Candidate { votes }),
            any_with::<LeaderState>((cluster_size, last_index))
                .prop_map(|leader_state| RoleState::Leader { leader_state }),
        ]
        .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

impl Arbitrary for LeaderState {
    type Parameters = (usize, Index);

    fn arbitrary_with((cluster_size, last_index): Self::Parameters) -> Self::Strategy {
        (
            prop::collection::vec(1..=(last_index + 1), cluster_size),
            prop::collection::vec(0..=last_index, cluster_size),
        )
            .prop_map(|(next_index, match_index)| {
                let next_index = next_index
                    .into_iter()
                    .map(|index| NonZeroIndex::new(index).unwrap())
                    .collect();
                LeaderState {
                    next_index,
                    match_index,
                }
            })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

impl<C: Command + Arbitrary + 'static> Arbitrary for Message<C>
where
    <C as Arbitrary>::Strategy: 'static,
{
    type Parameters = (usize, Index, Term);

    fn arbitrary_with((num_nodes, last_index, current_term): Self::Parameters) -> Self::Strategy {
        prop_oneof![
            any_with::<InterNodeMessage<C>>((num_nodes, last_index, current_term))
                .prop_map(|inter_node_message| { Message::InterNodeMessage(inter_node_message) }),
            Just(Message::BecomeCandidate),
            any::<C>().prop_map(|cmd| Message::Command(cmd))
        ]
        .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

impl<C: Command + Arbitrary + 'static> Arbitrary for InterNodeMessage<C>
where
    <C as Arbitrary>::Strategy: 'static,
{
    type Parameters = (usize, Index, Term);

    fn arbitrary_with((num_nodes, last_index, current_term): Self::Parameters) -> Self::Strategy {
        prop_oneof![
            any_with::<AppendEntriesRequest<C>>((num_nodes, current_term)).prop_map(
                |append_entries_request| {
                    InterNodeMessage::AppendEntriesRequest(append_entries_request)
                }
            ),
            any_with::<AppendEntriesResponse>((num_nodes, last_index, current_term)).prop_map(
                |append_entries_response| {
                    InterNodeMessage::AppendEntriesResponse(append_entries_response)
                }
            ),
            any_with::<VoteRequest>((num_nodes, current_term))
                .prop_map(|vote_request| InterNodeMessage::VoteRequest(vote_request)),
            any_with::<VoteResponse>(current_term)
                .prop_map(|vote_response| InterNodeMessage::VoteResponse(vote_response)),
        ]
        .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

impl<C: Command + Arbitrary> Arbitrary for AppendEntriesRequest<C>
where
    <C as Arbitrary>::Strategy: 'static,
{
    type Parameters = (usize, Term);

    fn arbitrary_with((num_nodes, current_term): Self::Parameters) -> Self::Strategy {
        (
            any_message_term_with(current_term),
            0..num_nodes,
            any::<Index>(),
        )
            .prop_flat_map(|(term, leader_id, prev_log_index)| {
                let prev_log_term = 0..=term;
                // Limit entries to 5 to avoid proptest timeouts.
                let entries = prop::collection::vec(any_with::<LogEntry<C>>(term), 0..5);
                (
                    Just(term),
                    Just(leader_id as NodeId),
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
    type Parameters = (usize, Index, Term);

    fn arbitrary_with((num_nodes, last_index, current_term): Self::Parameters) -> Self::Strategy {
        (
            any_message_term_with(current_term),
            any::<bool>(),
            0..num_nodes,
            1..=(last_index + 1),
        )
            .prop_flat_map(|(term, success, node_id, next_index)| {
                let next_index = if success {
                    Just(Some(NonZeroIndex::new(next_index).unwrap()))
                } else {
                    Just(None)
                };
                (
                    Just(term),
                    Just(success),
                    Just(node_id as NodeId),
                    next_index,
                )
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
    type Parameters = (usize, Term);

    fn arbitrary_with((num_nodes, current_term): Self::Parameters) -> Self::Strategy {
        (
            // Vote requests are more interesting at non-equal terms, so generate a small range
            // with equal probability around the current term.
            current_term - 3..current_term + 3,
            0..num_nodes,
            any::<Index>(),
        )
            .prop_flat_map(|(term, candidate_id, last_log_index)| {
                let last_log_term = (0..=term).boxed();
                (
                    Just(term),
                    Just(candidate_id as NodeId),
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

impl Arbitrary for VoteResponse {
    type Parameters = Term;

    fn arbitrary_with(current_term: Self::Parameters) -> Self::Strategy {
        (any_message_term_with(current_term), any::<bool>())
            .prop_map(|(term, vote_granted)| VoteResponse { term, vote_granted })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

/// Generates a term for a message with an 80% chance of being the current term, 10% of being the
/// previous term, and 10% of a later term.
fn any_message_term_with(current_term: Term) -> BoxedStrategy<Term> {
    prop_oneof![
        8 => Just(current_term),
        1 => ..current_term,
        1 => current_term+1..,
    ]
    .boxed()
}

proptest! {
    #[test]
    fn raft_single_node_state_machine(TestCase {mut node, messages } in any::<TestCase>()) {
        // Check invariants at the beginning to make sure the test is valid.
        check_invariants(&node, &node.model())?;

        for message in messages {
            let model = node.model();

            // Execute operation on the node. We ignore returned outgoing messages; we only assert
            // invariants.
            match message {
                Message::InterNodeMessage(message) => {
                    let _ = node.handle_inter_node_message(message);
                }
                Message::Command(command) => {
                    let _ = node.handle_command(command);
                }
                Message::BecomeCandidate => {
                    let _ = node.become_candidate();
                }
            }

            // Check invariants after each step.
            check_invariants(&node, &model)?;
        }
    }
}
