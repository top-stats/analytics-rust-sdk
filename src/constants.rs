use std::time::Duration;

/// Every limit here mirrors a server-side value; the server is authoritative.
pub const DEFAULT_HOST: &str = "https://topstats.gg";
pub const EVENTS_PATH: &str = "/v1/events";
pub const FLAGS_PATH: &str = "/v1/flags/evaluate";

pub const MAX_BATCH_SIZE: usize = 500;
pub const MAX_EVENT_BYTES: usize = 65_536;
pub const MAX_BODY_BYTES: usize = 2_097_152;

pub const MAX_NAME_LENGTH: usize = 128;
pub const MAX_PROPERTY_KEY_LENGTH: usize = 128;
pub const MAX_SOURCE_LENGTH: usize = 128;
pub const MAX_ACTOR_LENGTH: usize = 256;
pub const MAX_ACTOR_LABEL_LENGTH: usize = 256;
pub const MAX_FLAG_KEYS: usize = 200;
pub const MAX_FLAG_ACTOR_LENGTH: usize = 200;

pub const DEFAULT_FLUSH_AT: usize = 20;
pub const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
pub const DEFAULT_MAX_RETRIES: u32 = 3;
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_MAX_QUEUE_SIZE: usize = 10_000;

pub const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(500);
pub const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);

pub const SDK_NAME: &str = "topstats-rust";
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
