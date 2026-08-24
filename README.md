# TopStats Analytics for Rust

The official Rust client for [TopStats Analytics](https://topstats.gg). It
buffers your events in memory, sends them in batches from a background thread,
retries transient failures, and never panics or returns errors from `capture` -
failures surface through an error handler you control.

Not on crates.io yet. Install from the repository:

```toml
[dependencies]
topstats-analytics = { git = "https://github.com/top-stats/analytics-rust-sdk" }
```

## Quick start

```rust
use topstats_analytics::{CaptureOptions, Client};
use serde_json::{json, Map};

let client = Client::new(std::env::var("TOPSTATS_KEY").expect("set TOPSTATS_KEY"))?;

let mut properties = Map::new();
properties.insert("mode".to_owned(), json!("survival"));
properties.insert("playtime".to_owned(), json!(42));

client.capture("player_join", Some(properties), CaptureOptions::default());

// Events are buffered and sent in batches; flush before your process exits.
client.shutdown();
# Ok::<(), topstats_analytics::Error>(())
```

Send `playtime` as an unquoted number on purpose: properties are stored by
their JSON type, so `42` can be summed and averaged while `"42"` can only be
grouped.

## Configuration

```rust
use std::sync::Arc;
use std::time::Duration;
use topstats_analytics::Client;

let client = Client::builder("ts_live_your_key")
    .host("https://topstats.gg")
    .flush_at(20)
    .flush_interval(Duration::from_secs(5))
    .max_retries(3)
    .timeout(Duration::from_secs(10))
    .default_source("shard-1")
    .max_queue_size(10_000)
    .on_error(Arc::new(|error| eprintln!("topstats: {error}")))
    .build()?;
# Ok::<(), topstats_analytics::Error>(())
```

| Option | Default | What it does |
| --- | --- | --- |
| `host` | `https://topstats.gg` | API origin. Also read from the `TOPSTATS_HOST` env var; a blank value is treated as unset. |
| `flush_at` | 20 | Buffered events that trigger a background send. |
| `flush_interval` | 5s | How often the background thread flushes regardless of volume. |
| `max_retries` | 3 | Retries after the first attempt, for 429, 5xx, and network errors only. |
| `timeout` | 10s | Per-request HTTP timeout. |
| `default_source` | unset | `_source` applied to events that do not set their own. |
| `max_queue_size` | 10000 | Buffer bound. When full, the oldest events are dropped and reported. |
| `on_error` | logs to stderr | Receives every failure `capture` swallows. |

## Events

```rust
use topstats_analytics::{CaptureOptions, Timestamp};

client.capture(
    "purchase",
    None,
    CaptureOptions {
        actor: Some("user_123".to_owned()),
        actor_label: Some("Ada Lovelace".to_owned()),
        source: Some("eu-west-1".to_owned()),
        timestamp: Some(Timestamp::System(std::time::SystemTime::now())),
        ..Default::default()
    },
);
```

- `actor` attributes the event to a player, user, or server and powers the
  Actors view and retention. `actor_label` is an optional display name.
- `timestamp` accepts a `SystemTime`, which the SDK formats correctly, or a
  string already in Z-suffixed ISO 8601 form. Strings with a UTC offset (even
  `+00:00`) are rejected client-side because the API rejects them.
- Unset fields are omitted from the payload entirely.

## Batching and errors

Events are serialised once at capture time and sent in batches capped at 500
events and 2 MiB per request, whichever comes first. A single event over
65536 bytes is dropped and reported, never sent. Failed sends retry with
jittered exponential backoff, honour `Retry-After`, and give up after
`max_retries`; 400, 401, 402, and 413 are never retried. `capture` itself
never blocks on the network.

## Feature flags

```rust
use topstats_analytics::EvaluateInput;

if client.is_enabled("new-checkout", EvaluateInput {
    actor_key: Some("user_123".to_owned()),
    ..Default::default()
}) {
    // the new path
}
```

`is_enabled` returns `false` on any failure, so it is always safe to branch
on. `evaluate` returns the full per-flag results and surfaces errors.

## Shutdown

Call `client.shutdown()` before your process exits; it flushes the buffer and
stops the background thread, and is safe to call twice. Dropping the client
does the same as a best effort.

## Limits

| Limit | Value |
| --- | --- |
| Events per request | 500 |
| Bytes per event | 65536 |
| Bytes per request body | 2097152 |
| Requests per minute per address | 6000 |

Full product documentation: <https://docs.topstats.gg/docs/analytics>
