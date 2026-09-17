use serde_json::{Map, Value};

use crate::{
    constants::{
        MAX_ACTOR_LABEL_LENGTH, MAX_ACTOR_LENGTH, MAX_EVENT_BYTES, MAX_NAME_LENGTH,
        MAX_PROPERTY_KEY_LENGTH, MAX_SOURCE_LENGTH,
    },
    error::Error,
    timestamp::{now_wire_timestamp, to_wire_timestamp, Timestamp},
};

/// Optional per-event context for `capture`. All fields default to unset.
#[derive(Debug, Clone, Default)]
pub struct CaptureOptions {
    pub actor: Option<String>,
    pub actor_label: Option<String>,
    pub source: Option<String>,
    pub timestamp: Option<Timestamp>,
}

pub struct SerialisedEvent {
    pub json: String,
    pub bytes: usize,
}

/// Builds the wire object from an explicit allowlist of the six fields the API
/// accepts, so nothing extra can ever reach the server's strict schema.
/// Serialises exactly once, at enqueue time.
pub fn serialise_event(
    name: &str,
    properties: Option<Map<String, Value>>,
    options: &CaptureOptions,
    default_source: Option<&str>,
) -> Result<SerialisedEvent, Error> {
    validate_name(name)?;

    let mut wire = Map::new();
    wire.insert("name".to_owned(), Value::String(name.to_owned()));

    if let Some(map) = properties {
        validate_property_keys(&map)?;
        wire.insert("properties".to_owned(), Value::Object(map));
    }

    let source = options.source.as_deref().or(default_source);

    if let Some(value) = source {
        validate_length(value, MAX_SOURCE_LENGTH, "_source")?;
        wire.insert("_source".to_owned(), Value::String(value.to_owned()));
    }

    if let Some(value) = options.actor.as_deref() {
        validate_length(value, MAX_ACTOR_LENGTH, "_actor")?;
        wire.insert("_actor".to_owned(), Value::String(value.to_owned()));
    }

    if let Some(value) = options.actor_label.as_deref() {
        validate_length(value, MAX_ACTOR_LABEL_LENGTH, "_actorLabel")?;
        wire.insert("_actorLabel".to_owned(), Value::String(value.to_owned()));
    }

    // Stamp unset timestamps at capture time, not send time, so an event that
    // waits in the buffer keeps the moment it actually happened.
    let wire_timestamp = match options.timestamp.as_ref() {
        Some(timestamp) => to_wire_timestamp(timestamp)?,
        None => now_wire_timestamp()?,
    };
    wire.insert("_timestamp".to_owned(), Value::String(wire_timestamp));

    let json = serde_json::to_string(&Value::Object(wire)).map_err(|serialise_error| {
        Error::Validation {
            message: format!("event does not serialise to JSON: {serialise_error}"),
        }
    })?;

    let bytes = json.len();

    if bytes > MAX_EVENT_BYTES {
        return Err(Error::EventTooLarge {
            name: name.to_owned(),
            bytes,
            limit: MAX_EVENT_BYTES,
        });
    }

    Ok(SerialisedEvent { json, bytes })
}

fn validate_name(name: &str) -> Result<(), Error> {
    if name.is_empty() || name.len() > MAX_NAME_LENGTH {
        return Err(Error::Validation {
            message: format!("event name must be 1 to {MAX_NAME_LENGTH} characters"),
        });
    }

    Ok(())
}

fn validate_length(value: &str, limit: usize, field: &str) -> Result<(), Error> {
    if value.len() > limit {
        return Err(Error::Validation {
            message: format!("{field} must be at most {limit} characters"),
        });
    }

    Ok(())
}

fn validate_property_keys(properties: &Map<String, Value>) -> Result<(), Error> {
    if properties.keys().any(|key| key.is_empty() || key.len() > MAX_PROPERTY_KEY_LENGTH) {
        return Err(Error::Validation {
            message: format!("property keys must be 1 to {MAX_PROPERTY_KEY_LENGTH} characters"),
        });
    }

    Ok(())
}
