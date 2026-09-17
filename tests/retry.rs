mod common;

use std::{sync::Arc, time::Duration};

use topstats_analytics::CaptureOptions;

use common::{client_with, ErrorCollector, FakeTransport, Scripted, SleepRecorder};

fn accepted() -> Scripted {
    Scripted::Ok {
        status: 202,
        body: "{\"accepted\":1}".to_owned(),
    }
}

fn status(code: u16) -> Scripted {
    Scripted::Ok {
        status: code,
        body: format!("{{\"statusCode\":{code},\"error\":\"err\",\"message\":\"scripted\"}}"),
    }
}

#[test]
fn retries_429_then_succeeds() {
    let transport = FakeTransport::scripted(vec![status(429), accepted()]);
    let collector = ErrorCollector::new();
    let sleeps = SleepRecorder::new();
    let client = client_with(Arc::clone(&transport), &collector, sleeps.as_sleeper());

    client.capture("event", None, CaptureOptions::default());
    client.flush();

    assert_eq!(transport.request_count(), 2);
    assert!(collector.messages().is_empty());
    assert_eq!(sleeps.slept.lock().expect("lock").len(), 1);
}

#[test]
fn retries_5xx_then_succeeds() {
    let transport = FakeTransport::scripted(vec![status(503), status(500), accepted()]);
    let collector = ErrorCollector::new();
    let client = client_with(
        Arc::clone(&transport),
        &collector,
        SleepRecorder::new().as_sleeper(),
    );

    client.capture("event", None, CaptureOptions::default());
    client.flush();

    assert_eq!(transport.request_count(), 3);
    assert!(collector.messages().is_empty());
}

#[test]
fn retries_network_errors() {
    let transport = FakeTransport::scripted(vec![Scripted::NetworkError, accepted()]);
    let collector = ErrorCollector::new();
    let client = client_with(
        Arc::clone(&transport),
        &collector,
        SleepRecorder::new().as_sleeper(),
    );

    client.capture("event", None, CaptureOptions::default());
    client.flush();

    assert_eq!(transport.request_count(), 2);
    assert!(collector.messages().is_empty());
}

#[test]
fn honours_retry_after_seconds() {
    let transport = FakeTransport::scripted(vec![
        Scripted::OkWithRetryAfter {
            status: 429,
            body: "{}".to_owned(),
            retry_after: "7".to_owned(),
        },
        accepted(),
    ]);
    let collector = ErrorCollector::new();
    let sleeps = SleepRecorder::new();
    let client = client_with(Arc::clone(&transport), &collector, sleeps.as_sleeper());

    client.capture("event", None, CaptureOptions::default());
    client.flush();

    let slept = sleeps.slept.lock().expect("lock").clone();
    assert_eq!(slept, vec![Duration::from_secs(7)]);
}

#[test]
fn permanent_statuses_are_never_retried() {
    for code in [400_u16, 401, 402, 413] {
        let transport = FakeTransport::scripted(vec![status(code), accepted()]);
        let collector = ErrorCollector::new();
        let sleeps = SleepRecorder::new();
        let client = client_with(Arc::clone(&transport), &collector, sleeps.as_sleeper());

        client.capture("event", None, CaptureOptions::default());
        client.flush();

        assert_eq!(transport.request_count(), 1, "status {code} must not retry");
        assert!(sleeps.slept.lock().expect("lock").is_empty());

        let messages = collector.messages();
        assert_eq!(messages.len(), 1);
        assert!(messages[0].contains(&code.to_string()));
    }
}

#[test]
fn gives_up_after_max_retries_and_reports() {
    let transport = FakeTransport::scripted(vec![status(503)]);
    let collector = ErrorCollector::new();
    let client = client_with(
        Arc::clone(&transport),
        &collector,
        SleepRecorder::new().as_sleeper(),
    );

    client.capture("event", None, CaptureOptions::default());
    client.flush();

    // 1 initial + 3 default retries.
    assert_eq!(transport.request_count(), 4);
    assert_eq!(collector.messages().len(), 1);
}

#[test]
fn the_api_key_never_appears_in_error_output() {
    let transport = FakeTransport::scripted(vec![Scripted::NetworkError]);
    let collector = ErrorCollector::new();
    let client = client_with(
        Arc::clone(&transport),
        &collector,
        SleepRecorder::new().as_sleeper(),
    );

    client.capture("event", None, CaptureOptions::default());
    client.flush();

    for message in collector.messages() {
        assert!(!message.contains("ts_test_fake_key_for_unit_tests_only"));
    }
}
