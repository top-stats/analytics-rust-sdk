//! Official TopStats Analytics SDK for Rust.
//!
//! Buffers events in memory, sends them in batches from a background thread,
//! retries transient failures, and never raises into caller code from
//! `capture`. See the README for a walkthrough and
//! <https://docs.topstats.gg/docs/analytics> for the product docs.

mod client;
mod constants;
mod error;
mod event;
mod flags;
mod queue;
mod timestamp;
mod transport;

pub use client::{Client, ClientBuilder};
pub use constants::{
    DEFAULT_HOST, MAX_BATCH_SIZE, MAX_BODY_BYTES, MAX_EVENT_BYTES, VERSION,
};
pub use error::{Error, ErrorHandler};
pub use event::CaptureOptions;
pub use flags::{EvaluateInput, FlagResult};
pub use timestamp::Timestamp;
pub use transport::{Sleeper, Transport, TransportResponse};
