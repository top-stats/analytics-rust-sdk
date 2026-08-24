use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::Error;

/// A timestamp for `capture`: either a concrete instant or a string already in
/// the wire form the API accepts.
#[derive(Debug, Clone)]
pub enum Timestamp {
    System(SystemTime),
    /// Must already be Z-suffixed ISO 8601 with a four-digit year. Strings
    /// carrying a UTC offset (even `+00:00`) are rejected client-side because
    /// the API rejects them with a 400.
    Text(String),
}

impl From<SystemTime> for Timestamp {
    fn from(value: SystemTime) -> Self {
        Timestamp::System(value)
    }
}

impl From<String> for Timestamp {
    fn from(value: String) -> Self {
        Timestamp::Text(value)
    }
}

impl From<&str> for Timestamp {
    fn from(value: &str) -> Self {
        Timestamp::Text(value.to_owned())
    }
}

pub(crate) fn to_wire_timestamp(timestamp: &Timestamp) -> Result<String, Error> {
    match timestamp {
        Timestamp::System(instant) => format_system_time(*instant),
        Timestamp::Text(text) => validate_wire_text(text),
    }
}

pub(crate) fn now_wire_timestamp() -> Result<String, Error> {
    format_system_time(SystemTime::now())
}

fn format_system_time(instant: SystemTime) -> Result<String, Error> {
    let since_epoch = instant
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Validation {
            message: "timestamps before 1970 are not supported".to_owned(),
        })?;

    let (date, time_of_day) = split_epoch(since_epoch);

    // The API requires a four-digit year, so anything past 9999 must be
    // rejected here rather than sent as an expanded-year form the server 400s.
    if date.year > 9999 {
        return Err(Error::Validation {
            message: "timestamp year is outside the supported 1970-9999 range".to_owned(),
        });
    }

    Ok(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        date.year,
        date.month,
        date.day,
        time_of_day.hours,
        time_of_day.minutes,
        time_of_day.seconds,
        time_of_day.milliseconds,
    ))
}

struct CivilDate {
    year: i64,
    month: u32,
    day: u32,
}

struct TimeOfDay {
    hours: u64,
    minutes: u64,
    seconds: u64,
    milliseconds: u64,
}

fn split_epoch(since_epoch: Duration) -> (CivilDate, TimeOfDay) {
    let total_seconds = since_epoch.as_secs();
    let milliseconds = u64::from(since_epoch.subsec_millis());

    let days = total_seconds / 86_400;
    let seconds_in_day = total_seconds % 86_400;

    let time_of_day = TimeOfDay {
        hours: seconds_in_day / 3_600,
        minutes: (seconds_in_day % 3_600) / 60,
        seconds: seconds_in_day % 60,
        milliseconds,
    };

    (civil_from_days(days as i64), time_of_day)
}

/// Howard Hinnant's civil-from-days algorithm; exact over the full range.
fn civil_from_days(days_since_epoch: i64) -> CivilDate {
    let z = days_since_epoch + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let adjusted_year = if month <= 2 { year + 1 } else { year };

    CivilDate {
        year: adjusted_year,
        month: month as u32,
        day: day as u32,
    }
}

/// Accept only what the API accepts: `YYYY-MM-DDTHH:MM:SS[.fff...]Z` with a
/// four-digit year. Everything else, including offset forms, is a client-side
/// validation error so the caller learns why instead of getting a 400.
fn validate_wire_text(text: &str) -> Result<String, Error> {
    let bytes = text.as_bytes();

    let looks_like_wire_form = bytes.len() >= 20
        && bytes[bytes.len() - 1] == b'Z'
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[10] == b'T'
        && bytes[13] == b':'
        && bytes[16] == b':'
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && digits_at(bytes, 5, 7)
        && digits_at(bytes, 8, 10)
        && digits_at(bytes, 11, 13)
        && digits_at(bytes, 14, 16)
        && digits_at(bytes, 17, 19)
        && fraction_is_valid(&bytes[19..bytes.len() - 1]);

    if !looks_like_wire_form {
        return Err(Error::Validation {
            message: "timestamp strings must be Z-suffixed ISO 8601 with a four-digit year \
                      (offsets like +00:00 are rejected by the API); pass a SystemTime to have \
                      the SDK format it"
                .to_owned(),
        });
    }

    Ok(text.to_owned())
}

fn digits_at(bytes: &[u8], start: usize, end: usize) -> bool {
    bytes[start..end].iter().all(u8::is_ascii_digit)
}

fn fraction_is_valid(middle: &[u8]) -> bool {
    if middle.is_empty() {
        return true;
    }

    middle[0] == b'.' && middle.len() > 1 && middle[1..].iter().all(u8::is_ascii_digit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_formats_to_z_form() {
        let formatted = format_system_time(UNIX_EPOCH).expect("epoch formats");
        assert_eq!(formatted, "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn known_instant_formats_correctly() {
        let instant = UNIX_EPOCH + Duration::from_millis(1_767_225_600_123);
        let formatted = format_system_time(instant).expect("formats");
        assert_eq!(formatted, "2026-01-01T00:00:00.123Z");
    }

    #[test]
    fn year_past_9999_is_rejected() {
        let far_future = UNIX_EPOCH + Duration::from_secs(300_000_000_000);
        assert!(format_system_time(far_future).is_err());
    }

    #[test]
    fn offset_strings_are_rejected() {
        assert!(validate_wire_text("2026-01-01T00:00:00+00:00").is_err());
        assert!(validate_wire_text("2026-01-01T00:00:00.000+02:00").is_err());
    }

    #[test]
    fn z_form_passes_through() {
        assert!(validate_wire_text("2026-01-01T00:00:00Z").is_ok());
        assert!(validate_wire_text("2026-01-01T00:00:00.000Z").is_ok());
        assert!(validate_wire_text("9999-12-31T23:59:59.999Z").is_ok());
    }

    #[test]
    fn expanded_year_strings_are_rejected() {
        assert!(validate_wire_text("+275760-09-13T00:00:00.000Z").is_err());
        assert!(validate_wire_text("-000001-01-01T00:00:00.000Z").is_err());
    }
}
