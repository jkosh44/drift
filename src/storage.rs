//! Module responsible for persisting the state of a Raft node.

use crate::state::{Command, LogEntry, LogState, NodeId, NonZeroIndex, Term};

/// Trait to persist state of a Raft node.
pub trait Storage<C: Command> {
    /// Read the existing state if it exists.
    fn read_state(&self) -> Option<LogState<C>>;
    /// Update the metadata of a Raft node.
    fn persist_metadata(&self, current_term: Term, voted_for: Option<NodeId>);
    /// Persist new log entries.
    fn extend_log(&self, log: &[LogEntry<C>]);
    /// Truncate existing log entries.
    fn truncate_log(&self, start: NonZeroIndex);
}
