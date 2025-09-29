//! Simulation facade module.
//!
//! This module exists to make it easy to write code that can run either on
//! the real Tokio runtime or inside a deterministic network simulator like
//! [turmoil]. The goal is to centralize the conditional compilation and
//! re-exports so the rest of the code can be written against a single API
//! surface.

mod net {
    #[cfg(not(feature = "turmoil"))]
    #[expect(unused)]
    pub use tokio::net::*;

    #[cfg(feature = "turmoil")]
    #[expect(unused)]
    pub use turmoil::net::*;
}
