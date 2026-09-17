use std::collections::HashMap;

use serde::Deserialize;
use serde_json::{Map, Value};

use crate::{constants::{MAX_FLAG_ACTOR_LENGTH, MAX_FLAG_KEYS}, error::Error};

/// Input for `evaluate`. Every field is optional; keys are only required to be
/// non-empty strings - the API imposes no charset or length rule on them.
#[derive(Debug, Clone, Default)]
pub struct EvaluateInput {
    pub actor_key: Option<String>,
    pub group_key: Option<String>,
    pub keys: Option<Vec<String>>,
    pub log_exposure: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct FlagResult {
    pub value: bool,
    pub variant: String,
    pub reason: String,
}

#[derive(Deserialize)]
pub(crate) struct EvaluateResponse {
    pub flags: HashMap<String, FlagResult>,
}

pub(crate) fn build_evaluate_body(input: &EvaluateInput) -> Result<String, Error> {
    let mut body = Map::new();

    if let Some(actor_key) = normalised(&input.actor_key, "actorKey")? {
        body.insert("actorKey".to_owned(), Value::String(actor_key));
    }

    if let Some(group_key) = normalised(&input.group_key, "groupKey")? {
        body.insert("groupKey".to_owned(), Value::String(group_key));
    }

    if let Some(keys) = input.keys.as_ref() {
        if keys.len() > MAX_FLAG_KEYS {
            return Err(Error::Validation {
                message: format!("keys may contain at most {MAX_FLAG_KEYS} entries"),
            });
        }

        for key in keys {
            if key.trim().is_empty() {
                return Err(Error::Validation {
                    message: "flag keys must be non-empty".to_owned(),
                });
            }
        }

        let values = keys.iter().cloned().map(Value::String).collect();
        body.insert("keys".to_owned(), Value::Array(values));
    }

    if let Some(log_exposure) = input.log_exposure {
        body.insert("logExposure".to_owned(), Value::Bool(log_exposure));
    }

    serde_json::to_string(&Value::Object(body)).map_err(|serialise_error| Error::Validation {
        message: format!("evaluate input does not serialise: {serialise_error}"),
    })
}

fn normalised(field: &Option<String>, name: &str) -> Result<Option<String>, Error> {
    let Some(raw) = field.as_deref() else {
        return Ok(None);
    };

    let trimmed = raw.trim();

    if trimmed.is_empty() || trimmed.len() > MAX_FLAG_ACTOR_LENGTH {
        return Err(Error::Validation {
            message: format!("{name} must be 1 to {MAX_FLAG_ACTOR_LENGTH} characters"),
        });
    }

    Ok(Some(trimmed.to_owned()))
}
