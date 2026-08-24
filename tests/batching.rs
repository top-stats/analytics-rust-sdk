mod common;

use std::sync::Arc;

use serde_json::{json, Map, Value};
use topstats_analytics::{CaptureOptions, MAX_BATCH_SIZE, MAX_BODY_BYTES};

use common::{client_with, ErrorCollector, FakeTransport, SleepRecorder};

#[test]
fn batches_split_at_the_event_count_cap() {
    let transport = FakeTransport::always_accepted();
    let collector = ErrorCollector::new();
    let client = client_with(
        Arc::clone(&transport),
        &collector,
        SleepRecorder::new().as_sleeper(),
    );

    for _ in 0..(MAX_BATCH_SIZE + 1) {
        client.capture("tick", None, CaptureOptions::default());
    }
    client.flush();

    assert_eq!(transport.request_count(), 2);

    let first: Value = serde_json::from_str(&transport.request(0).body).expect("json");
    let second: Value = serde_json::from_str(&transport.request(1).body).expect("json");
    assert_eq!(
        first["events"].as_array().expect("array").len(),
        MAX_BATCH_SIZE
    );
    assert_eq!(second["events"].as_array().expect("array").len(), 1);
}

#[test]
fn batches_split_at_the_body_byte_limit() {
    let transport = FakeTransport::always_accepted();
    let collector = ErrorCollector::new();
    let client = client_with(
        Arc::clone(&transport),
        &collector,
        SleepRecorder::new().as_sleeper(),
    );

    // Forty events of ~60KB each stay under the per-event cap but sum to
    // ~2.4MB, which cannot share one 2 MiB body.
    let payload = "x".repeat(60_000);

    for _ in 0..40 {
        let mut map = Map::new();
        map.insert("blob".to_owned(), json!(payload.clone()));
        client.capture("big", Some(map), CaptureOptions::default());
    }
    client.flush();

    assert!(transport.request_count() >= 2);

    for index in 0..transport.request_count() {
        assert!(transport.request(index).body.len() <= MAX_BODY_BYTES);
    }

    assert!(collector.messages().is_empty());
}

#[test]
fn an_oversized_event_is_dropped_and_reported_never_sent() {
    let transport = FakeTransport::always_accepted();
    let collector = ErrorCollector::new();
    let client = client_with(
        Arc::clone(&transport),
        &collector,
        SleepRecorder::new().as_sleeper(),
    );

    let mut map = Map::new();
    map.insert("blob".to_owned(), json!("x".repeat(70_000)));
    client.capture("huge", Some(map), CaptureOptions::default());
    client.flush();

    assert_eq!(transport.request_count(), 0);

    let messages = collector.messages();
    assert_eq!(messages.len(), 1);
    assert!(messages[0].contains("huge"));
    assert!(messages[0].contains("dropped"));
}
