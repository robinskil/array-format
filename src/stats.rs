//! Per-array statistics stored in an optional `.stats` sidecar.
//!
//! A [`StatsFile`] holds [`ArrayStats`] (e.g. min/max as [`StatValue`]) for the
//! arrays in a file. It is serialized to its own sidecar with an `ARST` trailer,
//! mirroring the footer format, so stats can be read without touching the data.

use rkyv::{Archive, Deserialize, Serialize};

use crate::dtype::DType;
use crate::error::{Error, Result};
use crate::layout::FillValue;
use crate::storage::Storage;

const MAGIC: [u8; 4] = *b"ARST";
const TRAILER_SIZE: usize = 12;

/// A typed min or max value.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub enum StatValue {
    /// Signed integer value (for the signed integer dtypes).
    Int(i64),
    /// Unsigned integer value (for the unsigned integer dtypes).
    UInt(u64),
    /// Floating-point value (for `Float32`/`Float64`).
    Float(f64),
    /// String or binary: raw bytes in lexicographic order.
    Bytes(Vec<u8>),
    /// Nanoseconds since the Unix epoch — matches [`DType::TimestampNs`].
    TimestampNs(i64),
}

/// Aggregate statistics for a single array covering all its chunks.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct ArrayStats {
    /// Name of the array these statistics describe.
    pub name: String,
    /// Global min across all chunks; `None` for unsupported dtypes.
    pub min: Option<StatValue>,
    /// Global max across all chunks; `None` for unsupported dtypes.
    pub max: Option<StatValue>,
    /// Count of elements equal to the array's fill value; 0 if none set.
    pub null_count: u64,
    /// Total element count across all chunks.
    pub row_count: u64,
}

impl ArrayStats {
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            min: None,
            max: None,
            null_count: 0,
            row_count: 0,
        }
    }
}

/// Current `.stats` sidecar format version. Bumped whenever the encoded shape
/// changes, so a sidecar written by an older build is rejected with a clear
/// error instead of being misread.
pub const STATS_VERSION: u32 = 1;

/// The stats file: one [`ArrayStats`] per array.
///
/// Stored in `{stem}.stats` alongside the `.af` file using the same
/// rkyv + trailer format as the footer:
/// `[rkyv_bytes][size: u64 LE][MAGIC: b"ARST"]`
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct StatsFile {
    /// Format version; see [`STATS_VERSION`].
    pub version: u32,
    /// Per-array statistics, one entry per array in the file.
    pub arrays: Vec<ArrayStats>,
}

impl Default for StatsFile {
    fn default() -> Self {
        Self {
            version: STATS_VERSION,
            arrays: Vec::new(),
        }
    }
}

impl StatsFile {
    pub(crate) fn upsert(&mut self, new_stats: ArrayStats) {
        if let Some(existing) = self.arrays.iter_mut().find(|a| a.name == new_stats.name) {
            *existing = new_stats;
        } else {
            self.arrays.push(new_stats);
        }
    }

    /// Drops the statistics for `name`, if present. Returns whether an entry
    /// was removed.
    ///
    /// Needed because a logically deleted array never re-enters the dirty set
    /// (`mark_deleted` clears its chunk list), so without an explicit removal
    /// its stale stats would survive every flush until the next compaction.
    pub(crate) fn remove(&mut self, name: &str) -> bool {
        let before = self.arrays.len();
        self.arrays.retain(|a| a.name != name);
        self.arrays.len() != before
    }

    /// Returns the statistics for the array named `name`, if present.
    pub fn get_array(&self, name: &str) -> Option<&ArrayStats> {
        self.arrays.iter().find(|a| a.name == name)
    }

    /// All entries in the file, in storage order.
    ///
    /// The bulk counterpart to [`get_array`](Self::get_array): building a
    /// column over N arrays via `get_array` is O(N²), since each call scans the
    /// whole `Vec`. Callers assembling a cross-array index should iterate once
    /// through this instead.
    pub fn entries(&self) -> &[ArrayStats] {
        &self.arrays
    }

    pub(crate) fn serialize(&self) -> Result<Vec<u8>> {
        let rkyv_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        let size = rkyv_bytes.len() as u64;
        let mut out = Vec::with_capacity(rkyv_bytes.len() + TRAILER_SIZE);
        out.extend_from_slice(&rkyv_bytes);
        out.extend_from_slice(&size.to_le_bytes());
        out.extend_from_slice(&MAGIC);
        Ok(out)
    }

    fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < TRAILER_SIZE {
            return Err(Error::InvalidFooter("stats data too short".into()));
        }
        let magic_start = data.len() - 4;
        if data[magic_start..] != MAGIC {
            return Err(Error::InvalidFooter("invalid stats magic".into()));
        }
        let size_start = magic_start - 8;
        let size = u64::from_le_bytes(data[size_start..magic_start].try_into().unwrap()) as usize;
        if size > size_start {
            return Err(Error::InvalidFooter("stats size exceeds data".into()));
        }
        let rkyv_start = size_start - size;
        let mut aligned: rkyv::util::AlignedVec = rkyv::util::AlignedVec::new();
        aligned.extend_from_slice(&data[rkyv_start..size_start]);
        let decoded: Self = rkyv::from_bytes::<Self, rkyv::rancor::Error>(&aligned)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        if decoded.version != STATS_VERSION {
            return Err(Error::InvalidFooter(format!(
                "unsupported stats version {} (expected {STATS_VERSION})",
                decoded.version
            )));
        }
        Ok(decoded)
    }
}

/// Reads and deserializes a stats file from `storage`.
pub(crate) async fn read_stats_file(storage: &(dyn Storage + Sync)) -> Result<StatsFile> {
    let file_size = storage.size().await?;
    if (file_size as usize) < TRAILER_SIZE {
        return Err(Error::InvalidFooter("stats file too short".into()));
    }
    let trailer = storage
        .read_range(file_size - TRAILER_SIZE as u64..file_size)
        .await?;
    if trailer[8..] != MAGIC {
        return Err(Error::InvalidFooter("invalid stats magic".into()));
    }
    let size = u64::from_le_bytes(trailer[..8].try_into().unwrap()) as usize;
    let total = size + TRAILER_SIZE;
    let start = file_size - total as u64;
    let data = storage.read_range(start..file_size).await?;
    StatsFile::deserialize(&data)
}

// ── Macros (must appear before use) ──────────────────────────────────────────

macro_rules! int_partial {
    ($bytes:expr, $fill:expr, $ty:ty) => {{
        let size = std::mem::size_of::<$ty>();
        let n = $bytes.len() / size;
        let fill_val: Option<$ty> = match $fill {
            Some(FillValue::Int(v)) => Some(*v as $ty),
            Some(FillValue::UInt(v)) => Some(*v as $ty),
            _ => None,
        };
        let mut min: Option<$ty> = None;
        let mut max: Option<$ty> = None;
        let mut null_count = 0u64;
        for i in 0..n {
            let e = <$ty>::from_le_bytes($bytes[i * size..(i + 1) * size].try_into().unwrap());
            if fill_val.map_or(false, |f| e == f) {
                null_count += 1;
            } else {
                min = Some(min.map_or(e, |m| m.min(e)));
                max = Some(max.map_or(e, |m| m.max(e)));
            }
        }
        (
            min.map(|v| StatValue::Int(v as i64)),
            max.map(|v| StatValue::Int(v as i64)),
            null_count,
            n as u64,
        )
    }};
}

macro_rules! uint_partial {
    ($bytes:expr, $fill:expr, $ty:ty) => {{
        let size = std::mem::size_of::<$ty>();
        let n = $bytes.len() / size;
        let fill_val: Option<$ty> = match $fill {
            Some(FillValue::UInt(v)) => Some(*v as $ty),
            Some(FillValue::Int(v)) => Some(*v as $ty),
            _ => None,
        };
        let mut min: Option<$ty> = None;
        let mut max: Option<$ty> = None;
        let mut null_count = 0u64;
        for i in 0..n {
            let e = <$ty>::from_le_bytes($bytes[i * size..(i + 1) * size].try_into().unwrap());
            if fill_val.map_or(false, |f| e == f) {
                null_count += 1;
            } else {
                min = Some(min.map_or(e, |m| m.min(e)));
                max = Some(max.map_or(e, |m| m.max(e)));
            }
        }
        (
            min.map(|v| StatValue::UInt(v as u64)),
            max.map(|v| StatValue::UInt(v as u64)),
            null_count,
            n as u64,
        )
    }};
}

macro_rules! float_partial {
    ($bytes:expr, $fill:expr, $ty:ty) => {{
        let size = std::mem::size_of::<$ty>();
        let n = $bytes.len() / size;
        let fill_val: Option<$ty> = match $fill {
            Some(FillValue::Float(v)) => Some(*v as $ty),
            _ => None,
        };
        let mut min: Option<$ty> = None;
        let mut max: Option<$ty> = None;
        let mut null_count = 0u64;
        for i in 0..n {
            let e = <$ty>::from_le_bytes($bytes[i * size..(i + 1) * size].try_into().unwrap());
            let is_fill =
                fill_val.map_or(false, |f: $ty| if f.is_nan() { e.is_nan() } else { e == f });
            if is_fill {
                null_count += 1;
            } else {
                min = Some(match min {
                    None => e,
                    Some(m) => {
                        if e.total_cmp(&m).is_lt() {
                            e
                        } else {
                            m
                        }
                    }
                });
                max = Some(match max {
                    None => e,
                    Some(m) => {
                        if e.total_cmp(&m).is_gt() {
                            e
                        } else {
                            m
                        }
                    }
                });
            }
        }
        (
            min.map(|v| StatValue::Float(v as f64)),
            max.map(|v| StatValue::Float(v as f64)),
            null_count,
            n as u64,
        )
    }};
}

// ── Computation ──────────────────────────────────────────────────────────────

/// Computes `(min, max, null_count, row_count)` for a single chunk's raw bytes.
///
/// `null_count` counts elements equal to `fill_value`; 0 when none is set.
/// Floats use `total_cmp` so NaN sorts last (max). `FixedSizeList` / `List`
/// return `(None, None, 0, 0)`.
pub(crate) fn compute_chunk_partial(
    bytes: &[u8],
    dtype: &DType,
    fill_value: Option<&FillValue>,
) -> (Option<StatValue>, Option<StatValue>, u64, u64) {
    if bytes.is_empty() {
        return (None, None, 0, 0);
    }
    match dtype {
        DType::Int8 => int_partial!(bytes, fill_value, i8),
        DType::Int16 => int_partial!(bytes, fill_value, i16),
        DType::Int32 => int_partial!(bytes, fill_value, i32),
        DType::Int64 => int_partial!(bytes, fill_value, i64),
        DType::UInt8 => uint_partial!(bytes, fill_value, u8),
        DType::UInt16 => uint_partial!(bytes, fill_value, u16),
        DType::UInt32 => uint_partial!(bytes, fill_value, u32),
        DType::UInt64 => uint_partial!(bytes, fill_value, u64),
        DType::Bool => bool_partial(bytes, fill_value),
        DType::Float32 => float_partial!(bytes, fill_value, f32),
        DType::Float64 => float_partial!(bytes, fill_value, f64),
        DType::String | DType::Binary => vlen_partial(bytes, fill_value),
        DType::TimestampNs => timestamp_partial(bytes, fill_value),
        DType::FixedSizeList { .. } | DType::List { .. } => (None, None, 0, 0),
    }
}

/// Merges one chunk's partial results into an aggregate [`ArrayStats`].
pub(crate) fn merge_partial(
    stats: &mut ArrayStats,
    min: Option<StatValue>,
    max: Option<StatValue>,
    null_count: u64,
    row_count: u64,
) {
    stats.null_count += null_count;
    stats.row_count += row_count;
    stats.min = stat_min(stats.min.take(), min);
    stats.max = stat_max(stats.max.take(), max);
}

// ── Internal helpers ─────────────────────────────────────────────────────────

fn stat_min(a: Option<StatValue>, b: Option<StatValue>) -> Option<StatValue> {
    match (a, b) {
        (None, x) | (x, None) => x,
        (Some(a), Some(b)) => Some(if stat_le(&a, &b) { a } else { b }),
    }
}

fn stat_max(a: Option<StatValue>, b: Option<StatValue>) -> Option<StatValue> {
    match (a, b) {
        (None, x) | (x, None) => x,
        (Some(a), Some(b)) => Some(if stat_le(&a, &b) { b } else { a }),
    }
}

fn stat_le(a: &StatValue, b: &StatValue) -> bool {
    match (a, b) {
        (StatValue::Int(a), StatValue::Int(b)) => a <= b,
        (StatValue::UInt(a), StatValue::UInt(b)) => a <= b,
        (StatValue::Float(a), StatValue::Float(b)) => a.total_cmp(b).is_le(),
        (StatValue::Bytes(a), StatValue::Bytes(b)) => a <= b,
        (StatValue::TimestampNs(a), StatValue::TimestampNs(b)) => a <= b,
        _ => false,
    }
}

fn bool_partial(
    bytes: &[u8],
    fill: Option<&FillValue>,
) -> (Option<StatValue>, Option<StatValue>, u64, u64) {
    let fill_val: Option<u8> = match fill {
        Some(FillValue::Bool(b)) => Some(u8::from(*b)),
        _ => None,
    };
    let mut min: Option<u8> = None;
    let mut max: Option<u8> = None;
    let mut null_count = 0u64;
    for &e in bytes {
        if fill_val == Some(e) {
            null_count += 1;
        } else {
            min = Some(min.map_or(e, |m| m.min(e)));
            max = Some(max.map_or(e, |m| m.max(e)));
        }
    }
    (
        min.map(|v| StatValue::UInt(v as u64)),
        max.map(|v| StatValue::UInt(v as u64)),
        null_count,
        bytes.len() as u64,
    )
}

fn vlen_partial(
    bytes: &[u8],
    fill: Option<&FillValue>,
) -> (Option<StatValue>, Option<StatValue>, u64, u64) {
    let n = find_vlen_count(bytes);
    if n == 0 {
        return (None, None, 0, 0);
    }
    let values_base = (n + 1) * 4;
    let fill_bytes: Option<&[u8]> = match fill {
        Some(FillValue::String(s)) => Some(s.as_bytes()),
        _ => None,
    };
    let mut min: Option<Vec<u8>> = None;
    let mut max: Option<Vec<u8>> = None;
    let mut null_count = 0u64;
    for i in 0..n {
        let start = u32::from_le_bytes(bytes[i * 4..i * 4 + 4].try_into().unwrap()) as usize;
        let end =
            u32::from_le_bytes(bytes[(i + 1) * 4..(i + 1) * 4 + 4].try_into().unwrap()) as usize;
        let val = &bytes[values_base + start..values_base + end];
        if fill_bytes == Some(val) {
            null_count += 1;
        } else {
            match &mut min {
                slot @ None => *slot = Some(val.to_vec()),
                Some(m) if val < m.as_slice() => *m = val.to_vec(),
                _ => {}
            }
            match &mut max {
                slot @ None => *slot = Some(val.to_vec()),
                Some(m) if val > m.as_slice() => *m = val.to_vec(),
                _ => {}
            }
        }
    }
    (
        min.map(StatValue::Bytes),
        max.map(StatValue::Bytes),
        null_count,
        n as u64,
    )
}

fn timestamp_partial(
    bytes: &[u8],
    fill: Option<&FillValue>,
) -> (Option<StatValue>, Option<StatValue>, u64, u64) {
    let n = bytes.len() / 8;
    let fill_val: Option<i64> = match fill {
        Some(FillValue::TimestampNs(v)) => Some(*v),
        Some(FillValue::Int(v)) => Some(*v),
        _ => None,
    };
    let mut min: Option<i64> = None;
    let mut max: Option<i64> = None;
    let mut null_count = 0u64;
    for i in 0..n {
        let e = i64::from_le_bytes(bytes[i * 8..(i + 1) * 8].try_into().unwrap());
        if fill_val == Some(e) {
            null_count += 1;
        } else {
            min = Some(min.map_or(e, |m| m.min(e)));
            max = Some(max.map_or(e, |m| m.max(e)));
        }
    }
    (
        min.map(StatValue::TimestampNs),
        max.map(StatValue::TimestampNs),
        null_count,
        n as u64,
    )
}

/// Determines the number of elements from an offset-buffer encoded chunk.
/// Uses the same algorithm as `decode_offsets` in `array.rs`.
fn find_vlen_count(bytes: &[u8]) -> usize {
    let mut n = 0usize;
    loop {
        let pos = (n + 1) * 4;
        if pos + 4 > bytes.len() {
            break;
        }
        let off = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        if pos + 4 + off == bytes.len() {
            n += 1;
            break;
        }
        n += 1;
    }
    n
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn i32_bytes(values: &[i32]) -> Vec<u8> {
        values.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    fn i64_bytes(values: &[i64]) -> Vec<u8> {
        values.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    fn f64_bytes(values: &[f64]) -> Vec<u8> {
        values.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    fn string_bytes(values: &[&str]) -> Vec<u8> {
        let mut offsets: Vec<u32> = vec![0];
        let mut data: Vec<u8> = Vec::new();
        for s in values {
            data.extend_from_slice(s.as_bytes());
            offsets.push(data.len() as u32);
        }
        let mut out: Vec<u8> = Vec::new();
        for o in &offsets {
            out.extend_from_slice(&o.to_le_bytes());
        }
        out.extend_from_slice(&data);
        out
    }

    #[test]
    fn i32_min_max_null_row_count() {
        // values: [3, 1, 4, 1, 5, 9], fill=1 — the two 1s count as nulls and are
        // excluded from min/max, so the range covers only non-fill elements.
        let bytes = i32_bytes(&[3, 1, 4, 1, 5, 9]);
        let (min, max, null_count, row_count) =
            compute_chunk_partial(&bytes, &DType::Int32, Some(&FillValue::Int(1)));
        assert_eq!(min, Some(StatValue::Int(3)));
        assert_eq!(max, Some(StatValue::Int(9)));
        assert_eq!(null_count, 2); // two 1s
        assert_eq!(row_count, 6);
    }

    #[test]
    fn i32_no_fill_value() {
        let bytes = i32_bytes(&[10, 20, 30]);
        let (min, max, null_count, row_count) = compute_chunk_partial(&bytes, &DType::Int32, None);
        assert_eq!(min, Some(StatValue::Int(10)));
        assert_eq!(max, Some(StatValue::Int(30)));
        assert_eq!(null_count, 0);
        assert_eq!(row_count, 3);
    }

    #[test]
    fn all_elements_equal_fill_value() {
        let bytes = i32_bytes(&[7, 7, 7]);
        let (_, _, null_count, row_count) =
            compute_chunk_partial(&bytes, &DType::Int32, Some(&FillValue::Int(7)));
        assert_eq!(null_count, row_count);
        assert_eq!(row_count, 3);
    }

    #[test]
    fn f64_min_max() {
        let bytes = f64_bytes(&[3.0, 1.0, 4.0, 1.5]);
        let (min, max, null_count, row_count) =
            compute_chunk_partial(&bytes, &DType::Float64, None);
        assert_eq!(min, Some(StatValue::Float(1.0)));
        assert_eq!(max, Some(StatValue::Float(4.0)));
        assert_eq!(null_count, 0);
        assert_eq!(row_count, 4);
    }

    #[test]
    fn string_lexicographic_min_max() {
        let bytes = string_bytes(&["banana", "apple", "cherry"]);
        let (min, max, null_count, row_count) = compute_chunk_partial(&bytes, &DType::String, None);
        assert_eq!(min, Some(StatValue::Bytes(b"apple".to_vec())));
        assert_eq!(max, Some(StatValue::Bytes(b"cherry".to_vec())));
        assert_eq!(null_count, 0);
        assert_eq!(row_count, 3);
    }

    #[test]
    fn string_fill_value_null_count() {
        let bytes = string_bytes(&["a", "", "b", ""]);
        let fill = FillValue::String(String::new());
        let (_, _, null_count, row_count) =
            compute_chunk_partial(&bytes, &DType::String, Some(&fill));
        assert_eq!(null_count, 2);
        assert_eq!(row_count, 4);
    }

    #[test]
    fn merge_partial_aggregates_correctly() {
        let mut stats = ArrayStats::new("x".into());
        merge_partial(
            &mut stats,
            Some(StatValue::Int(5)),
            Some(StatValue::Int(10)),
            1,
            3,
        );
        merge_partial(
            &mut stats,
            Some(StatValue::Int(2)),
            Some(StatValue::Int(8)),
            0,
            2,
        );
        assert_eq!(stats.min, Some(StatValue::Int(2)));
        assert_eq!(stats.max, Some(StatValue::Int(10)));
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.row_count, 5);
    }

    #[test]
    fn statsfile_serialize_deserialize_roundtrip() {
        let sf = StatsFile {
            version: STATS_VERSION,
            arrays: vec![ArrayStats {
                name: "arr".into(),
                min: Some(StatValue::Int(-1)),
                max: Some(StatValue::Int(99)),
                null_count: 3,
                row_count: 100,
            }],
        };
        let bytes = sf.serialize().unwrap();
        let restored = StatsFile::deserialize(&bytes).unwrap();
        assert_eq!(sf, restored);
    }

    #[test]
    fn statsfile_empty_roundtrip() {
        let sf = StatsFile::default();
        let bytes = sf.serialize().unwrap();
        let restored = StatsFile::deserialize(&bytes).unwrap();
        assert_eq!(sf, restored);
    }

    #[test]
    fn timestamp_min_max() {
        // values: [10, 20, -5, 7, 20] with fill=20 — two 20s are nulls, excluded
        // from min/max; min comes from -5, max from 10.
        let bytes = i64_bytes(&[10, 20, -5, 7, 20]);
        let (min, max, null_count, row_count) = compute_chunk_partial(
            &bytes,
            &DType::TimestampNs,
            Some(&FillValue::TimestampNs(20)),
        );
        assert_eq!(min, Some(StatValue::TimestampNs(-5)));
        assert_eq!(max, Some(StatValue::TimestampNs(10)));
        assert_eq!(null_count, 2);
        assert_eq!(row_count, 5);
    }

    #[test]
    fn timestamp_fill_value_int_fallback() {
        // FillValue::Int is accepted as a fallback for the TimestampNs path,
        // so a value matching it still counts as a null.
        let bytes = i64_bytes(&[1, 2, 3]);
        let (_min, _max, null_count, row_count) =
            compute_chunk_partial(&bytes, &DType::TimestampNs, Some(&FillValue::Int(2)));
        assert_eq!(null_count, 1);
        assert_eq!(row_count, 3);
    }
}
