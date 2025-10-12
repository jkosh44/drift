//! Module containing errors for raft.

use crate::state::NodeId;
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq, Clone)]
pub enum Error {
    /// Indicates that an operation is only allowed on a leader, but was attempted on a different
    /// role.
    ///
    /// Contains the [`NodeId`] of the current leader if it is known.
    #[error("invalid operation on non-leader node")]
    NotLeader(Option<NodeId>),
}
