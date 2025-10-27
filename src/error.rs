//! Module containing errors for raft.

use crate::state::NodeId;
use thiserror::Error;

/// Error from sending a command to a Raft node.
#[derive(Error, Debug, PartialEq, Eq, Clone)]
pub enum CommandError {
    /// Indicates that commands are only allowed on a leader, but was attempted on a different
    /// role.
    ///
    /// Contains the [`NodeId`] of the current leader if it is known.
    #[error("command not allowed on non-leader node")]
    NotLeader(Option<NodeId>),
}

/// Error from trying to transition a Raft node to the candidate role.
#[derive(Error, Debug, PartialEq, Eq, Clone)]
pub enum BecomeCandidateError {
    /// Indicates that a leader cannot be promoted to a candidate.
    #[error("cannot promote leader to candidate")]
    IsLeader,
}
