//! Nanosecond-precision timestamp wrapper.

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Nanoseconds since the Unix epoch (1970-01-01 00:00:00 UTC).
///
/// Layout-compatible with `i64` (`#[repr(transparent)]`); on-disk chunk
/// encoding is identical to a raw `i64` chunk.
#[repr(transparent)]
#[derive(
    Copy,
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    FromBytes,
    IntoBytes,
    KnownLayout,
    Immutable,
)]
pub struct TimestampNs(pub i64);

impl TimestampNs {
    /// Wraps a raw nanoseconds-since-epoch value.
    pub const fn new(nanos: i64) -> Self {
        Self(nanos)
    }

    /// Returns the underlying nanoseconds-since-epoch value.
    pub const fn nanos(self) -> i64 {
        self.0
    }
}

impl From<i64> for TimestampNs {
    fn from(v: i64) -> Self {
        Self(v)
    }
}

impl From<TimestampNs> for i64 {
    fn from(v: TimestampNs) -> i64 {
        v.0
    }
}
