//! Module containing the state of a raft node.

use std::fmt::Debug;
use std::num::NonZeroU64;
use std::ops::{Deref, RangeFrom};

use proptest::prelude::{Arbitrary, BoxedStrategy, Strategy, any};
use proptest_derive::Arbitrary;

macro_rules! wrapper_type {
    ($wrapper:ident, $inner:ty, $($traits:ident),*) => {
        #[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Arbitrary, $($traits,)*)]
        pub(crate) struct $wrapper($inner);

        impl Deref for $wrapper {
            type Target = $inner;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl From<$inner> for $wrapper {
            fn from(value: $inner) -> Self {
                $wrapper(value)
            }
        }

        impl From<$wrapper> for $inner {
            fn from(value: $wrapper) -> Self {
                value.0
            }
        }
    };
}

pub(crate) type Term = u64;
pub(crate) type NodeId = u64;
/// New type for Raft log Indexes. In Raft, the log uses 1-based indexing; the first entry in the
/// log has an index of 1. Often an index of 0 is used to indicate "outside the log", for example,
/// a previous log index of 0 means that there is no previous log.
///
/// Rust, like most languages, uses 0-based indexing from arrays and similar containers. The
/// convention in this code base is to use "index" when referring to the 1-based indexing of Raft
/// logs and "idx" when referring to the 0-based indexing of Rust containers.
pub(crate) type Index = u64;
wrapper_type!(NonZeroIndex, NonZeroU64, Copy);

impl NonZeroIndex {
    pub(crate) fn new(index: Index) -> Option<Self> {
        Some(Self(NonZeroU64::new(index)?))
    }

    pub(crate) fn into_idx(self) -> usize {
        (self.get() as usize) - 1
    }

    pub(crate) fn from_idx(idx: usize) -> Self {
        Self::new(idx as u64 + 1).expect("adding one ensures that the result is non-zero")
    }
}

impl From<NonZeroIndex> for Index {
    fn from(index: NonZeroIndex) -> Self {
        index.get()
    }
}

pub trait Command: Debug + Clone + Ord + PartialOrd + Eq + PartialEq {}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct LogEntry<C: Command> {
    pub(crate) term: Term,
    pub(crate) command: C,
}

/// Log entries; each entry contains a command for a state machine and a term when the leader
/// received the entry. The first index is 1.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct Log<C: Command> {
    log: Vec<LogEntry<C>>,
}

impl<C: Command> Log<C> {
    pub(crate) fn new() -> Self {
        Self { log: Vec::new() }
    }

    pub(crate) fn get(&self, index: Index) -> Option<&LogEntry<C>> {
        let index = NonZeroIndex::new(index)?;
        let idx: usize = index.into_idx();
        self.log.get(idx)
    }

    pub(crate) fn last(&self) -> Option<&LogEntry<C>> {
        self.log.last()
    }

    /// Truncate all logs entries, starting at and including index `start`.
    pub(crate) fn truncate(&mut self, start: NonZeroIndex) {
        let start_idx: usize = start.into_idx();
        self.log.truncate(start_idx);
    }

    pub(crate) fn extend(&mut self, entries: impl IntoIterator<Item = LogEntry<C>>) {
        self.log.extend(entries);
    }

    pub(crate) fn push(&mut self, entry: LogEntry<C>) {
        self.extend([entry])
    }

    /// The index of the next log that will be inserted.
    pub(crate) fn next_index(&self) -> NonZeroIndex {
        NonZeroIndex::from_idx(self.log.len())
    }

    /// The index of the last entry in the log.
    pub(crate) fn last_index(&self) -> Index {
        self.log.len() as Index
    }

    #[cfg(test)]
    pub(crate) fn inner(&self) -> &[LogEntry<C>] {
        &self.log
    }
}

impl<C: Command> std::ops::Index<RangeFrom<NonZeroIndex>> for Log<C> {
    type Output = [LogEntry<C>];

    fn index(&self, range: RangeFrom<NonZeroIndex>) -> &Self::Output {
        let start = range.start.into_idx();
        let range = RangeFrom { start };
        self.log.index(range)
    }
}

// TODO: All changes to this should be persisted.
/// Persistent state on all servers. Updated on stable storage before responding to RPCs.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct LogState<C: Command> {
    /// Latest term server has seen, increases monotonically.
    current_term: Term,
    /// Candidate ID that received vote in current term, if one exists.
    pub(crate) voted_for: Option<NodeId>,
    /// Log entries; each entry contains command for state machine, and term when entry was
    /// received by leader. First index is 1.
    pub(crate) log: Log<C>,
}

impl<L: Command> LogState<L> {
    pub(crate) fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: Log::new(),
        }
    }

    pub(crate) fn current_term(&self) -> &Term {
        &self.current_term
    }

    pub(crate) fn set_term(&mut self, term: Term) {
        self.voted_for = None;
        self.current_term = term;
    }

    pub(crate) fn increment_term(&mut self) {
        self.set_term(self.current_term + 1);
    }
}

/// Volatile state on all servers.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct NodeState {
    /// Index of highest log entry known to be committed, increases monotonically.
    pub(crate) commit_index: Index,
    /// Index of highest log entry applied to state machine, increases monotonically.
    pub(crate) last_applied: Index,
    /// ID of the current leader, if known.
    pub(crate) leader_id: Option<NodeId>,
}

impl NodeState {
    pub(crate) fn new() -> Self {
        Self {
            commit_index: 0,
            last_applied: 0,
            leader_id: None,
        }
    }
}

/// Volatile state on leaders.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct LeaderState<const N: usize> {
    /// For each server, index of the next log entry to send to that server.
    pub(crate) next_index: [NonZeroIndex; N],
    /// For each server, index of highest log entry known to be replicated on server, increases
    /// monotonically.
    pub(crate) match_index: [Index; N],
}

impl<const N: usize> LeaderState<N> {
    pub(crate) fn new(last_log_index: Index) -> Self {
        let next_index = NonZeroIndex::new(last_log_index + 1)
            .expect("adding one ensures that the result is non-zero");
        Self {
            next_index: [next_index; N],
            match_index: [0; N],
        }
    }
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
