use std::{sync::Arc, time::Duration};

use crate::{constants::{INITIAL_RETRY_DELAY, MAX_RETRY_AFTER, MAX_RETRY_DELAY}, error::Error};

pub struct TransportResponse {
    pub status: u16,
    pub body: String,
    pub retry_after: Option<String>,
}

/// The seam tests inject a fake through. The real implementation is
/// `UreqTransport`; nothing else in the crate touches the network.
pub trait Transport: Send + Sync {
    fn post(&self, url: &str, api_key: &str, body: &str) -> Result<TransportResponse, Error>;
}

/// How the retry loop waits between attempts. Public only so tests can inject
/// a recorder and observe backoff without actually sleeping.
pub type Sleeper = Arc<dyn Fn(Duration) + Send + Sync>;

pub(crate) struct UreqTransport {
    agent: ureq::Agent,
}

impl UreqTransport {
    pub fn new(timeout: Duration) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout(timeout)
            .user_agent(&format!(
                "{}/{}",
                crate::constants::SDK_NAME,
                crate::constants::VERSION
            ))
            .build();

        Self { agent }
    }
}

impl Transport for UreqTransport {
    fn post(&self, url: &str, api_key: &str, body: &str) -> Result<TransportResponse, Error> {
        let result = self
            .agent
            .post(url)
            .set("Authorization", &format!("Bearer {api_key}"))
            .set("Content-Type", "application/json")
            .send_string(body);

        match result {
            Ok(response) => Ok(read_response(response)),
            Err(ureq::Error::Status(_, response)) => Ok(read_response(response)),
            // The ureq error text can include the URL but never the
            // Authorization header, so the key cannot leak through it.
            Err(ureq::Error::Transport(transport_error)) => Err(Error::Network {
                message: transport_error.to_string(),
            }),
        }
    }
}

fn read_response(response: ureq::Response) -> TransportResponse {
    let status = response.status();
    let retry_after = response.header("Retry-After").map(str::to_owned);
    let body = response.into_string().unwrap_or_default();

    TransportResponse {
        status,
        body,
        retry_after,
    }
}

/// Sends one request with the retry policy: 429, 5xx, and network errors are
/// retried with jittered exponential backoff, honouring Retry-After; anything
/// else is returned immediately.
pub(crate) fn send_with_retries(
    transport: &dyn Transport,
    url: &str,
    api_key: &str,
    body: &str,
    max_retries: u32,
    sleeper: &Sleeper,
) -> Result<TransportResponse, Error> {
    let mut attempt: u32 = 0;

    loop {
        let outcome = transport.post(url, api_key, body);

        let error = match outcome {
            Ok(response) if response.status < 400 => return Ok(response),
            Ok(response) => api_error_from(&response),
            Err(network_error) => network_error,
        };

        if !error.is_retryable() || attempt >= max_retries {
            return Err(error);
        }

        sleeper(retry_delay(attempt, &error));
        attempt += 1;
    }
}

fn api_error_from(response: &TransportResponse) -> Error {
    Error::Api {
        status: response.status,
        message: response_message(&response.body),
        retry_after_seconds: response
            .retry_after
            .as_deref()
            .and_then(parse_retry_after_seconds),
    }
}

/// Surfaces the body's `message` field when the usual error shape is present,
/// otherwise the truncated raw body.
fn response_message(body: &str) -> String {
    let parsed: Result<serde_json::Value, _> = serde_json::from_str(body);

    if let Ok(value) = parsed {
        if let Some(message) = value.get("message").and_then(|field| field.as_str()) {
            return message.to_owned();
        }
    }

    let mut message = body.to_owned();

    if message.len() > 500 {
        message.truncate(500);
        message.push_str("...");
    }

    message
}

fn parse_retry_after_seconds(header: &str) -> Option<f64> {
    let seconds: f64 = header.trim().parse().ok()?;

    if seconds.is_finite() && seconds >= 0.0 {
        Some(seconds.min(MAX_RETRY_AFTER.as_secs_f64()))
    } else {
        None
    }
}

fn retry_delay(attempt: u32, error: &Error) -> Duration {
    if let Error::Api {
        retry_after_seconds: Some(seconds),
        ..
    } = error
    {
        return Duration::from_secs_f64(*seconds);
    }

    backoff_delay(attempt)
}

/// Half the window is fixed and half is pseudo-random. The rate limit is keyed
/// on the client address, so processes behind one egress IP hit the same 429
/// together; without jitter they would retry in lockstep and do it again.
fn backoff_delay(attempt: u32) -> Duration {
    let ceiling = INITIAL_RETRY_DELAY
        .saturating_mul(1_u32 << attempt.min(16))
        .min(MAX_RETRY_DELAY);

    let half = ceiling / 2;
    half + half.mul_f64(pseudo_random_unit())
}

/// Enough randomness to de-synchronise retries; cryptographic quality is not
/// needed, and this avoids a rand dependency.
fn pseudo_random_unit() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.subsec_nanos())
        .unwrap_or(0);

    f64::from(nanos % 1_000) / 1_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_is_capped() {
        assert_eq!(parse_retry_after_seconds("5"), Some(5.0));
        assert_eq!(
            parse_retry_after_seconds("3600"),
            Some(MAX_RETRY_AFTER.as_secs_f64())
        );
        assert_eq!(parse_retry_after_seconds("-1"), None);
        assert_eq!(parse_retry_after_seconds("soon"), None);
    }

    #[test]
    fn backoff_never_exceeds_the_ceiling() {
        for attempt in 0..40 {
            assert!(backoff_delay(attempt) <= MAX_RETRY_DELAY);
        }
    }

    #[test]
    fn message_extraction_prefers_the_message_field() {
        assert_eq!(
            response_message("{\"statusCode\":400,\"error\":\"Bad Request\",\"message\":\"nope\"}"),
            "nope"
        );
        assert_eq!(response_message("plain text"), "plain text");
    }
}
