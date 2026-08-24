mod common;

use std::sync::Arc;

use serde_json::{json, Map};
use topstats_analytics::{CaptureOptions, Client, Error};

use common::{client_with, ErrorCollector, FakeTransport, SleepRecorder};

#[test]
fn capture_never_panics_on_bad_input() {
    let transport = FakeTransport::always_accepted();
    let collector = ErrorCollector::new();
    let client = client_with(Arc::clone(&transport), &collector, SleepRecorder::new().as_sleeper());

    // Empty name, oversized name, oversized reserved fields, empty property
    // key: every one reports instead of panicking.
    client.capture("", None, CaptureOptions::default());
    client.capture(&"n".repeat(200), None, CaptureOptions::default());
    client.capture(
        "event",
        None,
        CaptureOptions {
            actor: Some("a".repeat(300)),
            ..CaptureOptions::default()
        },
    );

    let mut empty_key = Map::new();
    empty_key.insert(String::new(), json!(1));
    client.capture("event", Some(empty_key), CaptureOptions::default());

    client.flush();

    assert_eq!(transport.request_count(), 0);
    assert_eq!(collector.messages().len(), 4);
}

#[test]
fn queue_overflow_drops_the_oldest_and_reports() {
    let transport = FakeTransport::always_accepted();
    let collector = ErrorCollector::new();
    let sink = Arc::clone(&collector);

    let client = Client::builder("ts_test_fake_key_for_unit_tests_only")
        .transport(Arc::clone(&transport) as Arc<dyn topstats_analytics::Transport>)
        .sleeper(SleepRecorder::new().as_sleeper())
        .flush_at(1_000)
        .max_queue_size(2)
        .on_error(Arc::new(move |error| {
            sink.errors.lock().expect("errors lock").push(error.to_string());
        }))
        .build()
        .expect("client builds");

    client.capture("first", None, CaptureOptions::default());
    client.capture("second", None, CaptureOptions::default());
    client.capture("third", None, CaptureOptions::default());
    client.flush();
    client.shutdown();

    let body = transport.request(0).body;
    assert!(!body.contains("\"first\""));
    assert!(body.contains("\"second\""));
    assert!(body.contains("\"third\""));

    let messages = collector.messages();
    assert!(messages.iter().any(|message| message.contains("queue full")));
}

#[test]
fn validation_errors_are_typed() {
    let error = Error::Validation {
        message: "example".to_owned(),
    };
    assert!(!error.is_retryable());

    let rate_limited = Error::Api {
        status: 429,
        message: "slow down".to_owned(),
        retry_after_seconds: None,
    };
    assert!(rate_limited.is_retryable());

    let payment = Error::Api {
        status: 402,
        message: "cap".to_owned(),
        retry_after_seconds: None,
    };
    assert!(!payment.is_retryable());
}
