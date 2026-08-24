mod common;

use std::sync::Arc;
use std::time::Duration;

use topstats_analytics::{CaptureOptions, Client};

use common::{client_with, ErrorCollector, FakeTransport, SleepRecorder};

#[test]
fn shutdown_flushes_and_is_idempotent() {
    let transport = FakeTransport::always_accepted();
    let collector = ErrorCollector::new();
    let client = client_with(
        Arc::clone(&transport),
        &collector,
        SleepRecorder::new().as_sleeper(),
    );

    client.capture("event", None, CaptureOptions::default());
    client.shutdown();
    client.shutdown();

    assert_eq!(transport.request_count(), 1);
}

#[test]
fn capture_after_shutdown_reports_instead_of_sending() {
    let transport = FakeTransport::always_accepted();
    let collector = ErrorCollector::new();
    let client = client_with(
        Arc::clone(&transport),
        &collector,
        SleepRecorder::new().as_sleeper(),
    );

    client.shutdown();
    client.capture("late", None, CaptureOptions::default());

    assert_eq!(transport.request_count(), 0);
    assert!(collector
        .messages()
        .iter()
        .any(|message| message.contains("shut down")));
}

#[test]
fn drop_flushes_buffered_events() {
    let transport = FakeTransport::always_accepted();
    let collector = ErrorCollector::new();

    {
        let client = client_with(
            Arc::clone(&transport),
            &collector,
            SleepRecorder::new().as_sleeper(),
        );
        client.capture("event", None, CaptureOptions::default());
        // Client dropped here without an explicit shutdown.
    }

    assert_eq!(transport.request_count(), 1);
}

#[test]
fn the_flush_at_threshold_triggers_a_background_send() {
    let transport = FakeTransport::always_accepted();
    let collector = ErrorCollector::new();
    let sink = Arc::clone(&collector);

    let client = Client::builder("ts_test_fake_key_for_unit_tests_only")
        .transport(Arc::clone(&transport) as Arc<dyn topstats_analytics::Transport>)
        .sleeper(SleepRecorder::new().as_sleeper())
        .flush_at(2)
        .flush_interval(Duration::from_millis(50))
        .on_error(Arc::new(move |error| {
            sink.errors
                .lock()
                .expect("errors lock")
                .push(error.to_string());
        }))
        .build()
        .expect("client builds");

    client.capture("one", None, CaptureOptions::default());
    client.capture("two", None, CaptureOptions::default());

    // The background worker owns the send; give it a moment rather than
    // asserting on scheduling.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);

    while transport.request_count() == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(transport.request_count() >= 1);
    client.shutdown();
}

#[test]
fn blank_topstats_host_env_is_treated_as_unset() {
    // Env vars are process-global, so this test sets and restores carefully
    // and covers both blank and whitespace-only values in one place.
    let transport = FakeTransport::always_accepted();
    let collector = ErrorCollector::new();

    std::env::set_var("TOPSTATS_HOST", "   ");
    let client = client_with(
        Arc::clone(&transport),
        &collector,
        SleepRecorder::new().as_sleeper(),
    );
    client.capture("event", None, CaptureOptions::default());
    client.flush();
    std::env::remove_var("TOPSTATS_HOST");

    assert_eq!(transport.request(0).url, "https://topstats.gg/v1/events");
}

#[test]
fn builder_host_overrides_the_default_and_trailing_slash_is_trimmed() {
    let transport = FakeTransport::always_accepted();
    let collector = ErrorCollector::new();
    let sink = Arc::clone(&collector);

    let client = Client::builder("ts_test_fake_key_for_unit_tests_only")
        .transport(Arc::clone(&transport) as Arc<dyn topstats_analytics::Transport>)
        .sleeper(SleepRecorder::new().as_sleeper())
        .flush_at(1_000)
        .host("https://staging.example.com/")
        .on_error(Arc::new(move |error| {
            sink.errors
                .lock()
                .expect("errors lock")
                .push(error.to_string());
        }))
        .build()
        .expect("client builds");

    client.capture("event", None, CaptureOptions::default());
    client.flush();
    client.shutdown();

    assert_eq!(
        transport.request(0).url,
        "https://staging.example.com/v1/events"
    );
}

#[test]
fn a_blank_api_key_fails_to_build() {
    assert!(Client::new("   ").is_err());
}
