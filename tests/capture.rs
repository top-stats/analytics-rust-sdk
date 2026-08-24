mod common;

use std::sync::Arc;

use serde_json::{json, Map, Value};
use topstats_analytics::{CaptureOptions, Client};

use common::{client_with, ErrorCollector, FakeTransport, SleepRecorder};

fn properties(pairs: &[(&str, Value)]) -> Option<Map<String, Value>> {
    let mut map = Map::new();

    for (key, value) in pairs {
        map.insert((*key).to_owned(), value.clone());
    }

    Some(map)
}

fn parsed_events(body: &str) -> Vec<Value> {
    let parsed: Value = serde_json::from_str(body).expect("body is JSON");
    parsed["events"].as_array().expect("events array").clone()
}

#[test]
fn capture_sends_the_batch_shape_with_auth_headers() {
    let transport = FakeTransport::always_accepted();
    let collector = ErrorCollector::new();
    let client = client_with(
        Arc::clone(&transport),
        &collector,
        SleepRecorder::new().as_sleeper(),
    );

    client.capture(
        "player_join",
        properties(&[("mode", json!("survival"))]),
        CaptureOptions::default(),
    );
    client.flush();

    assert_eq!(transport.request_count(), 1);
    let request = transport.request(0);
    assert_eq!(request.url, "https://topstats.gg/v1/events");
    assert_eq!(request.api_key, "ts_test_fake_key_for_unit_tests_only");

    let events = parsed_events(&request.body);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["name"], "player_join");
    assert_eq!(events[0]["properties"]["mode"], "survival");
    assert!(collector.messages().is_empty());
}

#[test]
fn context_maps_to_the_underscore_fields() {
    let transport = FakeTransport::always_accepted();
    let collector = ErrorCollector::new();
    let client = client_with(
        Arc::clone(&transport),
        &collector,
        SleepRecorder::new().as_sleeper(),
    );

    client.capture(
        "purchase",
        None,
        CaptureOptions {
            actor: Some("user_123".to_owned()),
            actor_label: Some("Ada Lovelace".to_owned()),
            source: Some("eu-west-1".to_owned()),
            timestamp: Some("2026-01-01T00:00:00.000Z".into()),
        },
    );
    client.flush();

    let events = parsed_events(&transport.request(0).body);
    assert_eq!(events[0]["_actor"], "user_123");
    assert_eq!(events[0]["_actorLabel"], "Ada Lovelace");
    assert_eq!(events[0]["_source"], "eu-west-1");
    assert_eq!(events[0]["_timestamp"], "2026-01-01T00:00:00.000Z");
}

#[test]
fn unset_optionals_are_omitted_not_null() {
    let transport = FakeTransport::always_accepted();
    let collector = ErrorCollector::new();
    let client = client_with(
        Arc::clone(&transport),
        &collector,
        SleepRecorder::new().as_sleeper(),
    );

    client.capture("bare", None, CaptureOptions::default());
    client.flush();

    let events = parsed_events(&transport.request(0).body);
    let object = events[0].as_object().expect("event object");
    assert!(!object.contains_key("_actor"));
    assert!(!object.contains_key("_actorLabel"));
    assert!(!object.contains_key("_source"));
    assert!(!object.contains_key("properties"));
    // The SDK stamps a timestamp at capture time on purpose.
    assert!(object.contains_key("_timestamp"));
}

#[test]
fn default_source_applies_and_explicit_source_wins() {
    let transport = FakeTransport::always_accepted();
    let collector = ErrorCollector::new();
    let sink = Arc::clone(&collector);

    let client = Client::builder("ts_test_fake_key_for_unit_tests_only")
        .transport(Arc::clone(&transport) as Arc<dyn topstats_analytics::Transport>)
        .sleeper(SleepRecorder::new().as_sleeper())
        .flush_at(1_000)
        .default_source("shard-1")
        .on_error(Arc::new(move |error| {
            sink.errors
                .lock()
                .expect("errors lock")
                .push(error.to_string());
        }))
        .build()
        .expect("client builds");

    client.capture("first", None, CaptureOptions::default());
    client.capture(
        "second",
        None,
        CaptureOptions {
            source: Some("shard-2".to_owned()),
            ..CaptureOptions::default()
        },
    );
    client.flush();
    client.shutdown();

    let events = parsed_events(&transport.request(0).body);
    assert_eq!(events[0]["_source"], "shard-1");
    assert_eq!(events[1]["_source"], "shard-2");
}

#[test]
fn events_buffer_until_flush() {
    let transport = FakeTransport::always_accepted();
    let collector = ErrorCollector::new();
    let client = client_with(
        Arc::clone(&transport),
        &collector,
        SleepRecorder::new().as_sleeper(),
    );

    client.capture("one", None, CaptureOptions::default());
    client.capture("two", None, CaptureOptions::default());
    assert_eq!(transport.request_count(), 0);

    client.flush();
    assert_eq!(transport.request_count(), 1);
    assert_eq!(parsed_events(&transport.request(0).body).len(), 2);
}
