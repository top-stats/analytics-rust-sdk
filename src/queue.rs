use std::collections::VecDeque;

use crate::{constants::{MAX_BATCH_SIZE, MAX_BODY_BYTES}, event::SerialisedEvent};

// `{"events":[` + `]}` around the comma-joined events.
const WRAPPER_BYTES: usize = 13;

pub(crate) struct BoundedQueue {
    events: VecDeque<SerialisedEvent>,
    max_size: usize,
}

impl BoundedQueue {
    pub fn new(max_size: usize) -> Self {
        BoundedQueue {
            events: VecDeque::new(),
            max_size,
        }
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Pushes one event; when full, drops the OLDEST to make room and returns
    /// how many were dropped so the caller can report it.
    pub fn push(&mut self, event: SerialisedEvent) -> usize {
        let mut dropped = 0;

        while self.events.len() >= self.max_size {
            self.events.pop_front();
            dropped += 1;
        }

        self.events.push_back(event);
        dropped
    }

    /// Drains everything into request bodies, splitting at the batch-size cap
    /// and the body byte limit, whichever hits first. Byte accounting uses the
    /// real serialised sizes plus the wrapper and comma overhead.
    pub fn drain_batches(&mut self) -> Vec<String> {
        let mut batches = Vec::new();
        let mut current: Vec<SerialisedEvent> = Vec::new();
        let mut current_bytes = WRAPPER_BYTES;

        while let Some(event) = self.events.pop_front() {
            let separator = if current.is_empty() { 0 } else { 1 };
            let projected = current_bytes + separator + event.bytes;

            if !current.is_empty()
                && (current.len() >= MAX_BATCH_SIZE || projected > MAX_BODY_BYTES)
            {
                batches.push(build_body(&current));
                current.clear();
                current_bytes = WRAPPER_BYTES;
            }

            current_bytes += if current.is_empty() { 0 } else { 1 } + event.bytes;
            current.push(event);
        }

        if !current.is_empty() {
            batches.push(build_body(&current));
        }

        batches
    }
}

fn build_body(events: &[SerialisedEvent]) -> String {
    let mut body = String::from("{\"events\":[");

    for (index, event) in events.iter().enumerate() {
        if index > 0 {
            body.push(',');
        }

        body.push_str(&event.json);
    }

    body.push_str("]}");
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event_of(bytes: usize) -> SerialisedEvent {
        // A JSON string filling exactly `bytes`: {"name":"aaa..."} shape not
        // needed; the queue only reads .json and .bytes.
        let json = "x".repeat(bytes);
        SerialisedEvent { json, bytes }
    }

    #[test]
    fn splits_at_batch_size() {
        let mut queue = BoundedQueue::new(10_000);

        for _ in 0..(MAX_BATCH_SIZE + 1) {
            queue.push(event_of(10));
        }

        let batches = queue.drain_batches();
        assert_eq!(batches.len(), 2);
    }

    #[test]
    fn splits_at_body_byte_limit() {
        let mut queue = BoundedQueue::new(10_000);
        let big = MAX_BODY_BYTES / 2;

        queue.push(event_of(big));
        queue.push(event_of(big));
        queue.push(event_of(big));

        let batches = queue.drain_batches();
        assert!(batches.len() >= 2);

        for batch in &batches {
            assert!(batch.len() <= MAX_BODY_BYTES);
        }
    }

    #[test]
    fn overflow_drops_oldest() {
        let mut queue = BoundedQueue::new(2);
        queue.push(SerialisedEvent {
            json: "first".to_owned(),
            bytes: 5,
        });
        queue.push(SerialisedEvent {
            json: "second".to_owned(),
            bytes: 6,
        });
        let dropped = queue.push(SerialisedEvent {
            json: "third".to_owned(),
            bytes: 5,
        });

        assert_eq!(dropped, 1);
        let batches = queue.drain_batches();
        assert_eq!(batches.len(), 1);
        assert!(!batches[0].contains("first"));
        assert!(batches[0].contains("second"));
        assert!(batches[0].contains("third"));
    }

    #[test]
    fn wrapper_overhead_matches_reality() {
        let body = build_body(&[SerialisedEvent {
            json: "{}".to_owned(),
            bytes: 2,
        }]);
        assert_eq!(body, "{\"events\":[{}]}");
        assert_eq!(body.len(), WRAPPER_BYTES + 2);
    }
}
