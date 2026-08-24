mod common;

use std::sync::Arc;

use serde_json::Value;
use topstats_analytics::EvaluateInput;

use common::{client_with, ErrorCollector, FakeTransport, Scripted, SleepRecorder};

fn flags_response() -> Scripted {
    Scripted::Ok {
        status: 200,
        body: "{\"flags\":{\"new-checkout\":{\"value\":true,\"variant\":\"true\",\"reason\":\"rollout\"}}}"
            .to_owned(),
    }
}

#[test]
fn evaluate_sends_camel_case_fields_and_parses_the_response() {
    let transport = FakeTransport::scripted(vec![flags_response()]);
    let collector = ErrorCollector::new();
    let client = client_with(Arc::clone(&transport), &collector, SleepRecorder::new().as_sleeper());

    let flags = client
        .evaluate(EvaluateInput {
            actor_key: Some("  user_123  ".to_owned()),
            group_key: Some("team_9".to_owned()),
            keys: Some(vec!["new-checkout".to_owned()]),
            log_exposure: Some(false),
        })
        .expect("evaluate succeeds");

    let request = transport.request(0);
    assert_eq!(request.url, "https://topstats.gg/v1/flags/evaluate");

    let body: Value = serde_json::from_str(&request.body).expect("json");
    // actorKey is trimmed like the API trims it.
    assert_eq!(body["actorKey"], "user_123");
    assert_eq!(body["groupKey"], "team_9");
    assert_eq!(body["keys"][0], "new-checkout");
    assert_eq!(body["logExposure"], false);

    let result = flags.get("new-checkout").expect("flag present");
    assert!(result.value);
    assert_eq!(result.reason, "rollout");
}

#[test]
fn keys_with_colons_and_spaces_are_sent_not_rejected() {
    // The API has no charset rule on flag keys; inventing one client-side was
    // a real bug in an earlier SDK. These must reach the wire.
    let transport = FakeTransport::scripted(vec![flags_response()]);
    let collector = ErrorCollector::new();
    let client = client_with(Arc::clone(&transport), &collector, SleepRecorder::new().as_sleeper());

    let outcome = client.evaluate(EvaluateInput {
        keys: Some(vec!["billing:v2".to_owned(), "new checkout".to_owned()]),
        ..EvaluateInput::default()
    });

    assert!(outcome.is_ok());
    let body: Value = serde_json::from_str(&transport.request(0).body).expect("json");
    assert_eq!(body["keys"][0], "billing:v2");
    assert_eq!(body["keys"][1], "new checkout");
}

#[test]
fn is_enabled_returns_true_only_for_an_enabled_flag() {
    let transport = FakeTransport::scripted(vec![flags_response()]);
    let collector = ErrorCollector::new();
    let client = client_with(Arc::clone(&transport), &collector, SleepRecorder::new().as_sleeper());

    assert!(client.is_enabled("new-checkout", EvaluateInput::default()));
}

#[test]
fn is_enabled_is_false_on_any_failure() {
    let transport = FakeTransport::scripted(vec![Scripted::NetworkError]);
    let collector = ErrorCollector::new();
    let client = client_with(Arc::clone(&transport), &collector, SleepRecorder::new().as_sleeper());

    assert!(!client.is_enabled("anything", EvaluateInput::default()));
}

#[test]
fn is_enabled_is_false_for_a_missing_flag() {
    let transport = FakeTransport::scripted(vec![Scripted::Ok {
        status: 200,
        body: "{\"flags\":{}}".to_owned(),
    }]);
    let collector = ErrorCollector::new();
    let client = client_with(Arc::clone(&transport), &collector, SleepRecorder::new().as_sleeper());

    assert!(!client.is_enabled("unknown", EvaluateInput::default()));
}

#[test]
fn evaluate_rejects_over_200_keys_client_side() {
    let transport = FakeTransport::always_accepted();
    let collector = ErrorCollector::new();
    let client = client_with(Arc::clone(&transport), &collector, SleepRecorder::new().as_sleeper());

    let keys: Vec<String> = (0..201).map(|index| format!("flag-{index}")).collect();
    let outcome = client.evaluate(EvaluateInput {
        keys: Some(keys),
        ..EvaluateInput::default()
    });

    assert!(outcome.is_err());
    assert_eq!(transport.request_count(), 0);
}
