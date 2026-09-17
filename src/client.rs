use std::{collections::HashMap, sync::{atomic::{AtomicBool, Ordering}, Arc, Condvar, Mutex}, thread::JoinHandle, time::Duration};

use serde_json::{Map, Value};

use crate::{
    constants::{
        DEFAULT_FLUSH_AT, DEFAULT_FLUSH_INTERVAL, DEFAULT_HOST, DEFAULT_MAX_QUEUE_SIZE,
        DEFAULT_MAX_RETRIES, DEFAULT_TIMEOUT, EVENTS_PATH, FLAGS_PATH,
    },
    error::{default_error_handler, Error, ErrorHandler},
    event::{serialise_event, CaptureOptions},
    flags::{build_evaluate_body, EvaluateInput, EvaluateResponse, FlagResult},
    queue::BoundedQueue,
    transport::{send_with_retries, Sleeper, Transport, UreqTransport},
};

pub struct ClientBuilder {
    api_key: String,
    host: Option<String>,
    flush_at: usize,
    flush_interval: Duration,
    max_retries: u32,
    timeout: Duration,
    on_error: Option<ErrorHandler>,
    default_source: Option<String>,
    max_queue_size: usize,
    #[cfg(test)]
    transport: Option<Arc<dyn Transport>>,
    #[cfg(test)]
    sleeper: Option<Sleeper>,
}

impl ClientBuilder {
    #[must_use]
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    #[must_use]
    pub fn flush_at(mut self, flush_at: usize) -> Self {
        self.flush_at = flush_at.max(1);
        self
    }

    #[must_use]
    pub const fn flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    #[must_use]
    pub const fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    #[must_use]
    pub const fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    #[must_use]
    pub fn on_error(mut self, handler: ErrorHandler) -> Self {
        self.on_error = Some(handler);
        self
    }

    #[must_use]
    pub fn default_source(mut self, source: impl Into<String>) -> Self {
        self.default_source = Some(source.into());
        self
    }

    #[must_use]
    pub fn max_queue_size(mut self, max_queue_size: usize) -> Self {
        self.max_queue_size = max_queue_size.max(1);
        self
    }

    /// Replaces the HTTP layer. Exists so tests inject a fake and never touch
    /// the network; production code should not need it.
    #[cfg(test)]
    #[must_use]
    pub fn transport(mut self, transport: Arc<dyn Transport>) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Replaces the retry sleep. Exists so tests observe backoff without
    /// actually waiting.
    #[cfg(test)]
    #[must_use]
    pub fn sleeper(mut self, sleeper: Sleeper) -> Self {
        self.sleeper = Some(sleeper);
        self
    }

    pub fn build(self) -> Result<Client, Error> {
        if self.api_key.trim().is_empty() {
            return Err(Error::Validation {
                message: "an API key is required".to_owned(),
            });
        }

        let host = resolve_host(self.host);
        let timeout = self.timeout;

        #[cfg(test)]
        let transport = match self.transport {
            Some(injected) => injected,
            None => Arc::new(UreqTransport::new(timeout)),
        };

        #[cfg(not(test))]
        let transport = Arc::new(UreqTransport::new(timeout));

        #[cfg(test)]
        let sleeper: Sleeper = match self.sleeper {
            Some(injected) => injected,
            None => Arc::new(std::thread::sleep),
        };

        #[cfg(not(test))]
        let sleeper = Arc::new(std::thread::sleep);

        let on_error = self.on_error.unwrap_or_else(default_error_handler);

        let inner = Arc::new(Inner {
            api_key: self.api_key,
            events_url: format!("{host}{EVENTS_PATH}"),
            flags_url: format!("{host}{FLAGS_PATH}"),
            flush_at: self.flush_at,
            flush_interval: self.flush_interval,
            max_retries: self.max_retries,
            on_error,
            default_source: self.default_source,
            transport,
            sleeper,
            queue: Mutex::new(BoundedQueue::new(self.max_queue_size)),
            wake: Condvar::new(),
            shut_down: AtomicBool::new(false),
        });

        let worker = spawn_worker(Arc::clone(&inner));

        Ok(Client {
            inner,
            worker: Mutex::new(Some(worker)),
        })
    }
}

/// Builder host wins, then a non-blank TOPSTATS_HOST, then the default. A
/// blank env var is what an unset variable looks like in most container
/// runtimes, so it must not be treated as a host.
fn resolve_host(configured: Option<String>) -> String {
    let candidates = [
        configured,
        std::env::var("TOPSTATS_HOST").ok(),
        Some(DEFAULT_HOST.to_owned()),
    ];

    for candidate in candidates.into_iter().flatten() {
        let trimmed = candidate.trim();

        if !trimmed.is_empty() {
            return trimmed.trim_end_matches('/').to_owned();
        }
    }

    DEFAULT_HOST.to_owned()
}

struct Inner {
    api_key: String,
    events_url: String,
    flags_url: String,
    flush_at: usize,
    flush_interval: Duration,
    max_retries: u32,
    on_error: ErrorHandler,
    default_source: Option<String>,
    transport: Arc<dyn Transport>,
    sleeper: Sleeper,
    queue: Mutex<BoundedQueue>,
    wake: Condvar,
    shut_down: AtomicBool,
}

impl Inner {
    fn report(&self, error: &Error) {
        (self.on_error)(error);
    }

    fn drain_and_send(&self) {
        let batches = {
            let mut queue = match self.queue.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            queue.drain_batches()
        };

        for body in batches {
            let outcome = send_with_retries(
                self.transport.as_ref(),
                &self.events_url,
                &self.api_key,
                &body,
                self.max_retries,
                &self.sleeper,
            );

            if let Err(error) = outcome {
                self.report(&error);
            }
        }
    }
}

// The shutdown flag is only ever set while holding the queue mutex, and this
// loop only waits while holding it too, so a shutdown notification cannot be
// lost between the flag check and the wait - the classic lost-wakeup race
// that would otherwise leave join() blocked for a full flush interval.
fn spawn_worker(inner: Arc<Inner>) -> JoinHandle<()> {
    std::thread::spawn(move || loop {
        let should_exit = {
            let queue = match inner.queue.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };

            if inner.shut_down.load(Ordering::Acquire) {
                true
            } else {
                let (_guard, _timeout) = match inner.wake.wait_timeout(queue, inner.flush_interval)
                {
                    Ok(result) => result,
                    Err(poisoned) => poisoned.into_inner(),
                };

                inner.shut_down.load(Ordering::Acquire)
            }
        };

        inner.drain_and_send();

        if should_exit {
            return;
        }
    })
}

/// The TopStats Analytics client. Buffers events in memory, flushes them in
/// batches from a background thread, and never raises failures into caller
/// code - they surface through the on-error handler instead.
pub struct Client {
    inner: Arc<Inner>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl Client {
    pub fn new(api_key: impl Into<String>) -> Result<Self, Error> {
        Self::builder(api_key).build()
    }

    pub fn builder(api_key: impl Into<String>) -> ClientBuilder {
        ClientBuilder {
            api_key: api_key.into(),
            host: None,
            flush_at: DEFAULT_FLUSH_AT,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            max_retries: DEFAULT_MAX_RETRIES,
            timeout: DEFAULT_TIMEOUT,
            on_error: None,
            default_source: None,
            max_queue_size: DEFAULT_MAX_QUEUE_SIZE,
            #[cfg(test)]
            transport: None,
            #[cfg(test)]
            sleeper: None,
        }
    }

    /// Buffers one event. Never blocks on the network and never panics;
    /// anything wrong is reported through the on-error handler.
    pub fn capture(
        &self,
        name: &str,
        properties: Option<Map<String, Value>>,
        options: CaptureOptions,
    ) {
        if self.inner.shut_down.load(Ordering::Acquire) {
            self.inner.report(&Error::ShutDown);
            return;
        }

        let serialised = match serialise_event(
            name,
            properties,
            &options,
            self.inner.default_source.as_deref(),
        ) {
            Ok(event) => event,
            Err(error) => {
                self.inner.report(&error);
                return;
            }
        };

        let (dropped, should_flush) = {
            let mut queue = match self.inner.queue.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            let dropped = queue.push(serialised);
            (dropped, queue.len() >= self.inner.flush_at)
        };

        if dropped > 0 {
            self.inner.report(&Error::QueueOverflow { dropped });
        }

        if should_flush {
            self.inner.wake.notify_one();
        }
    }

    /// Sends everything buffered and returns when done. Send failures are
    /// reported through the on-error handler, exactly as background flushes
    /// report them.
    pub fn flush(&self) {
        self.inner.drain_and_send();
    }

    /// Flushes, stops the background thread, and joins it. Safe to call more
    /// than once; later calls are no-ops.
    pub fn shutdown(&self) {
        // Set the flag while holding the queue mutex so the worker cannot slip
        // between its flag check and its wait and miss the notification.
        let already_shut_down = {
            let _queue = match self.inner.queue.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            self.inner.shut_down.swap(true, Ordering::Release)
        };

        if already_shut_down {
            return;
        }

        self.inner.wake.notify_one();

        let handle = {
            let mut worker = match self.worker.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            worker.take()
        };

        if let Some(handle) = handle {
            let _ = handle.join();
        }

        // The worker drained on its way out, but events captured between its
        // final drain and the join land here.
        self.inner.drain_and_send();
    }

    /// Evaluates feature flags for an actor. Unlike `capture` this is a direct
    /// request-response call, so it returns errors instead of reporting them.
    pub fn evaluate(&self, input: EvaluateInput) -> Result<HashMap<String, FlagResult>, Error> {
        let body = build_evaluate_body(&input)?;

        let response = send_with_retries(
            self.inner.transport.as_ref(),
            &self.inner.flags_url,
            &self.inner.api_key,
            &body,
            self.inner.max_retries,
            &self.inner.sleeper,
        )?;

        let parsed: EvaluateResponse =
            serde_json::from_str(&response.body).map_err(|parse_error| Error::Api {
                status: response.status,
                message: format!("flag response did not parse: {parse_error}"),
                retry_after_seconds: None,
            })?;

        Ok(parsed.flags)
    }

    /// True only when the flag evaluates to enabled. Any failure - network,
    /// auth, unknown flag - is false, so this is always safe to branch on.
    pub fn is_enabled(&self, key: &str, input: EvaluateInput) -> bool {
        let mut narrowed = input;
        narrowed.keys = Some(vec![key.to_owned()]);

        self.evaluate(narrowed).is_ok_and(|flags| flags.get(key).is_some_and(|flag| flag.value))
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.shutdown();
    }
}
