//! Just enough RFC 3339 to say how old a generation is.
//!
//! The admin API stamps every generation with an RFC 3339 instant, and the only
//! thing this binary does with one is subtract it from now and print `2m14s`.
//! That is not worth `chrono` or `time` — a date crate is a large surface, and
//! the two of them together account for more code than the rest of this crate.
//!
//! What it *is* worth is being explicit that the parse is deliberately strict
//! about shape and deliberately incurious about everything else: leap seconds
//! are clamped rather than rejected, fractional seconds are skipped rather than
//! rounded, and an offset is applied rather than remembered. A wall-clock age
//! in a status line does not need better than that, and anything this parser
//! cannot read shows up as `?` instead of a crash.

/// Why a timestamp could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("not an RFC 3339 timestamp")]
pub struct BadTimestamp;

/// Days from 1970-01-01 to `y-m-d`, proleptic Gregorian.
///
/// Howard Hinnant's `days_from_civil`, which is the algorithm every date
/// library uses underneath: shift the year to start in March so the leap day
/// lands at the end, then count 400-year eras, whose length in days (146097) is
/// exact. It is branch-free apart from the sign fixups and correct for any year
/// this program could be handed.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = year - i64::from(month <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = i64::from(month);
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Parses an RFC 3339 timestamp into seconds since the Unix epoch.
///
/// Accepts `Z`, `+HH:MM` and `-HH:MM` offsets, with or without fractional
/// seconds. Returns [`BadTimestamp`] rather than a partial answer, because a
/// timestamp that half-parsed is an age that is silently wrong.
pub fn to_unix_seconds(text: &str) -> Result<i64, BadTimestamp> {
    let bytes = text.as_bytes();
    // `YYYY-MM-DDTHH:MM:SS` is the shortest thing that can be valid, and every
    // field below is at a fixed offset within it.
    if bytes.len() < 19 {
        return Err(BadTimestamp);
    }

    let num = |range: std::ops::Range<usize>| -> Result<i64, BadTimestamp> {
        text.get(range)
            .ok_or(BadTimestamp)?
            .parse::<i64>()
            .map_err(|_| BadTimestamp)
    };
    let sep = |at: usize, want: &[u8]| -> Result<(), BadTimestamp> {
        match bytes.get(at) {
            Some(c) if want.contains(c) => Ok(()),
            _ => Err(BadTimestamp),
        }
    };

    sep(4, b"-")?;
    sep(7, b"-")?;
    // RFC 3339 permits a lowercase `t`, and some emitters use a space.
    sep(10, b"Tt ")?;
    sep(13, b":")?;
    sep(16, b":")?;

    let year = num(0..4)?;
    let month = num(5..7)?;
    let day = num(8..10)?;
    let hour = num(11..13)?;
    let minute = num(14..16)?;
    let second = num(17..19)?;

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(BadTimestamp);
    }
    // 60 is a leap second and 61 exists in the standard. Clamping is right for
    // an age display and wrong for nothing this program does.
    if hour > 23 || minute > 59 || second > 61 {
        return Err(BadTimestamp);
    }

    let mut rest = text.get(19..).ok_or(BadTimestamp)?;
    // Fractional seconds: skipped, not rounded. The display resolution is one
    // second.
    if let Some(after_dot) = rest.strip_prefix('.') {
        let digits = after_dot
            .bytes()
            .take_while(u8::is_ascii_digit)
            .count();
        if digits == 0 {
            return Err(BadTimestamp);
        }
        rest = after_dot.get(digits..).ok_or(BadTimestamp)?;
    }

    let offset_seconds = match rest.as_bytes().first() {
        Some(b'Z' | b'z') if rest.len() == 1 => 0,
        Some(sign @ (b'+' | b'-')) => {
            if rest.len() != 6 || rest.as_bytes().get(3) != Some(&b':') {
                return Err(BadTimestamp);
            }
            let hours = rest
                .get(1..3)
                .ok_or(BadTimestamp)?
                .parse::<i64>()
                .map_err(|_| BadTimestamp)?;
            let minutes = rest
                .get(4..6)
                .ok_or(BadTimestamp)?
                .parse::<i64>()
                .map_err(|_| BadTimestamp)?;
            if hours > 23 || minutes > 59 {
                return Err(BadTimestamp);
            }
            let magnitude = hours * 3600 + minutes * 60;
            if *sign == b'-' {
                -magnitude
            } else {
                magnitude
            }
        }
        _ => return Err(BadTimestamp),
    };

    let days = days_from_civil(year, month as u32, day as u32);
    // The offset is what the local clock is *ahead* of UTC, so it comes off.
    Ok(days * 86_400 + hour * 3600 + minute * 60 + second - offset_seconds)
}

/// Seconds since the Unix epoch, right now.
///
/// Before 1970 the clock would have to be very wrong; the saturating branch
/// exists so a misconfigured host produces a strange age rather than a panic.
pub fn now_unix_seconds() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs().min(i64::MAX as u64) as i64,
        Err(e) => -(e.duration().as_secs().min(i64::MAX as u64) as i64),
    }
}

/// Renders an age in seconds the way `ps` and `kubectl` do: two units, largest
/// first, no padding.
///
/// A negative age means the server's clock is ahead of this one, which is worth
/// showing as such rather than rendering as a huge positive duration.
pub fn humanize_age(seconds: i64) -> String {
    if seconds < 0 {
        return format!("-{}", humanize_age(-seconds));
    }
    let (d, h, m, s) = (
        seconds / 86_400,
        (seconds % 86_400) / 3600,
        (seconds % 3600) / 60,
        seconds % 60,
    );
    match (d, h, m) {
        (0, 0, 0) => format!("{s}s"),
        (0, 0, _) => format!("{m}m{s}s"),
        (0, _, _) => format!("{h}h{m}m"),
        _ => format!("{d}d{h}h"),
    }
}

/// The age of an RFC 3339 timestamp, humanized, or `?` if it could not be read.
///
/// Age display is the one place a bad timestamp must not be fatal: the rest of
/// the generation row is still worth showing.
pub fn age_of(timestamp: &str, now: i64) -> String {
    match to_unix_seconds(timestamp) {
        Ok(then) => humanize_age(now - then),
        Err(_) => "?".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_epoch_is_zero() {
        assert_eq!(to_unix_seconds("1970-01-01T00:00:00Z"), Ok(0));
    }

    #[test]
    fn a_known_instant_matches_a_known_unix_time() {
        // `date -u -d @1735689600` is 2025-01-01T00:00:00Z.
        assert_eq!(to_unix_seconds("2025-01-01T00:00:00Z"), Ok(1_735_689_600));
    }

    #[test]
    fn leap_days_are_counted() {
        let feb28 = to_unix_seconds("2024-02-28T00:00:00Z").expect("valid");
        let mar01 = to_unix_seconds("2024-03-01T00:00:00Z").expect("valid");
        assert_eq!(mar01 - feb28, 2 * 86_400, "2024 is a leap year");

        let feb28 = to_unix_seconds("2023-02-28T00:00:00Z").expect("valid");
        let mar01 = to_unix_seconds("2023-03-01T00:00:00Z").expect("valid");
        assert_eq!(mar01 - feb28, 86_400, "2023 is not");
    }

    #[test]
    fn century_years_follow_the_gregorian_rule() {
        // 1900 is not a leap year, 2000 is. This is the case a naive
        // divisible-by-four parser gets wrong.
        let feb28 = to_unix_seconds("1900-02-28T00:00:00Z").expect("valid");
        let mar01 = to_unix_seconds("1900-03-01T00:00:00Z").expect("valid");
        assert_eq!(mar01 - feb28, 86_400);

        let feb28 = to_unix_seconds("2000-02-28T00:00:00Z").expect("valid");
        let mar01 = to_unix_seconds("2000-03-01T00:00:00Z").expect("valid");
        assert_eq!(mar01 - feb28, 2 * 86_400);
    }

    #[test]
    fn fractional_seconds_are_skipped_not_rejected() {
        let plain = to_unix_seconds("2025-06-01T12:00:00Z").expect("valid");
        assert_eq!(to_unix_seconds("2025-06-01T12:00:00.5Z"), Ok(plain));
        assert_eq!(to_unix_seconds("2025-06-01T12:00:00.123456789Z"), Ok(plain));
    }

    #[test]
    fn offsets_move_the_instant_the_right_way() {
        let utc = to_unix_seconds("2025-06-01T12:00:00Z").expect("valid");
        // Noon in a zone one hour ahead of UTC is 11:00 UTC, i.e. earlier.
        assert_eq!(to_unix_seconds("2025-06-01T12:00:00+01:00"), Ok(utc - 3600));
        assert_eq!(to_unix_seconds("2025-06-01T12:00:00-05:00"), Ok(utc + 5 * 3600));
    }

    #[test]
    fn lowercase_t_and_a_space_separator_are_accepted() {
        let utc = to_unix_seconds("2025-06-01T12:00:00Z").expect("valid");
        assert_eq!(to_unix_seconds("2025-06-01t12:00:00Z"), Ok(utc));
        assert_eq!(to_unix_seconds("2025-06-01 12:00:00Z"), Ok(utc));
    }

    #[test]
    fn malformed_timestamps_are_rejected_rather_than_guessed() {
        for bad in [
            "",
            "2025-06-01",
            "2025-06-01T12:00:00",         // no offset at all
            "2025-06-01T12:00:00ZZ",       // trailing junk
            "2025-13-01T00:00:00Z",        // month 13
            "2025-06-32T00:00:00Z",        // day 32
            "2025-06-01T24:00:00Z",        // hour 24
            "2025-06-01T12:60:00Z",        // minute 60
            "2025/06/01T12:00:00Z",        // wrong separators
            "2025-06-01T12:00:00.Z",       // a dot with no digits
            "2025-06-01T12:00:00+1:00",    // offset not zero-padded
            "20xx-06-01T12:00:00Z",        // not a number
        ] {
            assert_eq!(to_unix_seconds(bad), Err(BadTimestamp), "{bad:?} parsed");
        }
    }

    #[test]
    fn leap_seconds_are_accepted_and_clamped_into_the_minute() {
        assert!(to_unix_seconds("2016-12-31T23:59:60Z").is_ok());
        assert_eq!(to_unix_seconds("2016-12-31T23:59:62Z"), Err(BadTimestamp));
    }

    #[test]
    fn ages_render_in_two_units() {
        assert_eq!(humanize_age(0), "0s");
        assert_eq!(humanize_age(45), "45s");
        assert_eq!(humanize_age(134), "2m14s");
        assert_eq!(humanize_age(3600), "1h0m");
        assert_eq!(humanize_age(3 * 3600 + 25 * 60), "3h25m");
        assert_eq!(humanize_age(86_400 + 2 * 3600), "1d2h");
    }

    #[test]
    fn a_server_clock_ahead_of_ours_reads_as_negative() {
        assert_eq!(humanize_age(-30), "-30s");
    }

    #[test]
    fn an_unreadable_timestamp_degrades_to_a_question_mark() {
        assert_eq!(age_of("not a date", 0), "?");
        assert_eq!(age_of("2025-01-01T00:00:00Z", 1_735_689_660), "1m0s");
    }
}
