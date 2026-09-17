use std::{fmt, sync::Arc};

/// Every failure the SDK can surface. The API key is never included in any
/// variant, so no error can leak it through Display or Debug output.
#[derive(Debug, Clone)]
pub enum Error {
    /// The caller passed something the server would reject with a 400, caught
    /// client-side so the reason is readable.
    Validation { message: String },
    /// A single event serialised over the per-event byte limit and was dropped
    /// before sending.
    EventTooLarge {
        name: String,
        bytes: usize,
        limit: usize,
    },
    /// The bounded queue was full and the oldest events were dropped to make
    /// room.
    QueueOverflow { dropped: usize },
    /// The API answered with a non-success status.
    Api {
        status: u16,
        message: String,
        retry_after_seconds: Option<f64>,
    },
    /// The request never got an HTTP response.
    Network { message: String },
    /// The client is shut down and the call was ignored.
    ShutDown,
}

impl Error {
    /// Whether the transport would retry this failure: 429, 5xx, and network
    /// errors only. 400, 401, 402, and 413 are permanent.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        match self {
            Self::Api { status, .. } => *status == 429 || *status >= 500,
            Self::Network { .. } => true,
            _ => false,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Validation { message } => write!(formatter, "validation: {message}"),
            Self::EventTooLarge { name, bytes, limit } => write!(
                formatter,
                "event \"{name}\" is {bytes} bytes, over the {limit} byte limit; dropped"
            ),
            Self::QueueOverflow { dropped } => write!(
                formatter,
                "queue full; dropped the oldest {dropped} buffered event(s)"
            ),
            Self::Api {
                status, message, ..
            } => write!(formatter, "api returned {status}: {message}"),
            Self::Network { message } => write!(formatter, "network: {message}"),
            Self::ShutDown => write!(formatter, "client is shut down"),
        }
    }
}

impl std::error::Error for Error {}

/// Failures from `capture` are reported here instead of being raised into
/// caller code.
pub type ErrorHandler = Arc<dyn Fn(&Error) + Send + Sync>;

pub fn default_error_handler() -> ErrorHandler {
    Arc::new(|error| {
        eprintln!("[topstats] {error}");
    })
}
