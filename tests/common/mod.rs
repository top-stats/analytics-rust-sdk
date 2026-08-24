// Every integration test binary compiles this module and none of them uses
// all of it, so unused-item warnings here would fail clippy -D warnings.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use topstats_analytics::{Client, Error, Sleeper, Transport, TransportResponse};

#[derive(Clone)]
pub struct RecordedRequest {
    pub url: String,
    pub api_key: String,
    pub body: String,
}

pub enum Scripted {
    Ok {
        status: u16,
        body: String,
    },
    OkWithRetryAfter {
        status: u16,
        body: String,
        retry_after: String,
    },
    NetworkError,
}

/// A fake transport: records every request and replays scripted responses in
/// order, repeating the last one when the script runs out. Nothing here ever
/// opens a socket.
pub struct FakeTransport {
    pub requests: Mutex<Vec<RecordedRequest>>,
    script: Mutex<Vec<Scripted>>,
}

impl FakeTransport {
    pub fn always_accepted() -> Arc<FakeTransport> {
        FakeTransport::scripted(vec![Scripted::Ok {
            status: 202,
            body: "{\"accepted\":1}".to_owned(),
        }])
    }

    pub fn scripted(script: Vec<Scripted>) -> Arc<FakeTransport> {
        Arc::new(FakeTransport {
            requests: Mutex::new(Vec::new()),
            script: Mutex::new(script),
        })
    }

    pub fn request_count(&self) -> usize {
        self.requests.lock().expect("requests lock").len()
    }

    pub fn request(&self, index: usize) -> RecordedRequest {
        self.requests.lock().expect("requests lock")[index].clone()
    }
}

impl Transport for FakeTransport {
    fn post(&self, url: &str, api_key: &str, body: &str) -> Result<TransportResponse, Error> {
        self.requests
            .lock()
            .expect("requests lock")
            .push(RecordedRequest {
                url: url.to_owned(),
                api_key: api_key.to_owned(),
                body: body.to_owned(),
            });

        let mut script = self.script.lock().expect("script lock");
        let step = if script.len() > 1 {
            script.remove(0)
        } else {
            script_first(&script)
        };

        match step {
            Scripted::Ok { status, body } => Ok(TransportResponse {
                status,
                body,
                retry_after: None,
            }),
            Scripted::OkWithRetryAfter {
                status,
                body,
                retry_after,
            } => Ok(TransportResponse {
                status,
                body,
                retry_after: Some(retry_after),
            }),
            Scripted::NetworkError => Err(Error::Network {
                message: "connection refused (fake)".to_owned(),
            }),
        }
    }
}

fn script_first(script: &[Scripted]) -> Scripted {
    match &script[0] {
        Scripted::Ok { status, body } => Scripted::Ok {
            status: *status,
            body: body.clone(),
        },
        Scripted::OkWithRetryAfter {
            status,
            body,
            retry_after,
        } => Scripted::OkWithRetryAfter {
            status: *status,
            body: body.clone(),
            retry_after: retry_after.clone(),
        },
        Scripted::NetworkError => Scripted::NetworkError,
    }
}

pub struct SleepRecorder {
    pub slept: Mutex<Vec<Duration>>,
}

impl SleepRecorder {
    pub fn new() -> Arc<SleepRecorder> {
        Arc::new(SleepRecorder {
            slept: Mutex::new(Vec::new()),
        })
    }

    pub fn as_sleeper(self: &Arc<SleepRecorder>) -> Sleeper {
        let recorder = Arc::clone(self);
        Arc::new(move |duration| {
            recorder.slept.lock().expect("slept lock").push(duration);
        })
    }
}

/// Collects every error the client reports, for assertions.
pub struct ErrorCollector {
    pub errors: Mutex<Vec<String>>,
}

impl ErrorCollector {
    pub fn new() -> Arc<ErrorCollector> {
        Arc::new(ErrorCollector {
            errors: Mutex::new(Vec::new()),
        })
    }

    pub fn messages(&self) -> Vec<String> {
        self.errors.lock().expect("errors lock").clone()
    }
}

pub fn client_with(
    transport: Arc<FakeTransport>,
    collector: &Arc<ErrorCollector>,
    sleeper: Sleeper,
) -> Client {
    let sink = Arc::clone(collector);

    Client::builder("ts_test_fake_key_for_unit_tests_only")
        .transport(transport)
        .sleeper(sleeper)
        // A large flush_at so tests control exactly when sends happen.
        .flush_at(1_000)
        .flush_interval(Duration::from_secs(3_600))
        .on_error(Arc::new(move |error| {
            sink.errors
                .lock()
                .expect("errors lock")
                .push(error.to_string());
        }))
        .build()
        .expect("client builds")
}
