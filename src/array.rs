//! Typed array interface for reading and writing array data.
//!
//! This module provides an Arrow-style typed layer on top of the raw byte
//! storage. [`PrimitiveArray`] wraps fixed-width numeric data, while
//! [`BinaryArray`] and [`StringArray`] handle variable-length values.
//!
//! All concrete array types implement the [`ArrayData`] trait, which
//! provides access to the [`DType`] and the serialized byte representation.
//! Writers accept `&dyn ArrayData`, and readers can produce either a
//! concrete typed array or a `Box<dyn ArrayData>` via dynamic dispatch.

use std::marker::PhantomData;

use crate::dtype::DType;
use crate::error::{Error, Result};

// ── NativeType trait ────────────────────────────────────────────────

/// Maps a Rust primitive to its corresponding [`DType`].
///
/// This is a sealed trait — only the fixed-width primitives supported by
/// the format implement it.
pub trait NativeType: Copy + Send + Sync + 'static {
    /// The [`DType`] that describes this type in the footer metadata.
    const DTYPE: DType;
}

macro_rules! impl_native {
    ($ty:ty, $variant:expr) => {
        impl NativeType for $ty {
            const DTYPE: DType = $variant;
        }
    };
}

impl_native!(u8, DType::UInt8);
impl_native!(u16, DType::UInt16);
impl_native!(u32, DType::UInt32);
impl_native!(u64, DType::UInt64);
impl_native!(i8, DType::Int8);
impl_native!(i16, DType::Int16);
impl_native!(i32, DType::Int32);
impl_native!(i64, DType::Int64);
impl_native!(f32, DType::Float32);
impl_native!(f64, DType::Float64);

// ── ArrayData trait ─────────────────────────────────────────────────

/// Trait for typed array data that can be written to storage.
///
/// Concrete types: [`PrimitiveArray<T>`], [`BinaryArray`], [`StringArray`].
pub trait ArrayData: Send + Sync {
    /// The element type of this array.
    fn dtype(&self) -> &DType;

    /// The raw byte representation, ready for block packing.
    fn as_bytes(&self) -> &[u8];
}

// ── PrimitiveArray ──────────────────────────────────────────────────

/// A contiguous array of fixed-width elements.
///
/// The inner `Vec<u8>` stores the elements in native little-endian layout.
/// Use [`values()`](PrimitiveArray::values) to get a typed `&[T]` slice.
pub struct PrimitiveArray<T: NativeType> {
    dtype: DType,
    data: Vec<u8>,
    _phantom: PhantomData<T>,
}

impl<T: NativeType> PrimitiveArray<T> {
    /// Creates a new `PrimitiveArray` from a slice of values.
    pub fn from_slice(values: &[T]) -> Self {
        let byte_len = std::mem::size_of_val(values);
        let data =
            unsafe { std::slice::from_raw_parts(values.as_ptr() as *const u8, byte_len) }.to_vec();
        Self {
            dtype: T::DTYPE,
            data,
            _phantom: PhantomData,
        }
    }

    /// Creates a `PrimitiveArray` from raw bytes that were previously
    /// stored in the format.
    ///
    /// Returns an error if `bytes.len()` is not a multiple of the element size.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        let elem = std::mem::size_of::<T>();
        if elem > 0 && bytes.len() % elem != 0 {
            return Err(Error::InvalidFooter(format!(
                "byte length {} is not a multiple of element size {}",
                bytes.len(),
                elem,
            )));
        }
        Ok(Self {
            dtype: T::DTYPE,
            data: bytes,
            _phantom: PhantomData,
        })
    }

    /// Returns the elements as a typed slice.
    pub fn values(&self) -> &[T] {
        let elem = std::mem::size_of::<T>();
        if elem == 0 || self.data.is_empty() {
            return &[];
        }
        let len = self.data.len() / elem;
        unsafe { std::slice::from_raw_parts(self.data.as_ptr() as *const T, len) }
    }

    /// Returns the number of elements.
    pub fn len(&self) -> usize {
        let elem = std::mem::size_of::<T>();
        if elem == 0 {
            return 0;
        }
        self.data.len() / elem
    }

    /// Returns `true` if the array is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl<T: NativeType> ArrayData for PrimitiveArray<T> {
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

// ── BinaryArray ─────────────────────────────────────────────────────

/// A variable-length binary array.
///
/// Stored as a 4-byte little-endian offset buffer (`N + 1` entries)
/// followed by concatenated value bytes.
pub struct BinaryArray {
    dtype: DType,
    data: Vec<u8>,
}

impl BinaryArray {
    /// Creates a new `BinaryArray` from a list of byte slices.
    pub fn from_slices(values: &[&[u8]]) -> Self {
        let mut offsets: Vec<u32> = Vec::with_capacity(values.len() + 1);
        let mut buf: Vec<u8> = Vec::new();
        offsets.push(0);
        for v in values {
            buf.extend_from_slice(v);
            offsets.push(buf.len() as u32);
        }
        let mut data = Vec::with_capacity(offsets.len() * 4 + buf.len());
        for off in &offsets {
            data.extend_from_slice(&off.to_le_bytes());
        }
        data.extend_from_slice(&buf);
        Self {
            dtype: DType::Binary,
            data,
        }
    }

    /// Creates a `BinaryArray` from raw bytes (offsets + values already encoded).
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            dtype: DType::Binary,
            data: bytes,
        }
    }

    /// Returns the number of elements.
    pub fn len(&self) -> usize {
        if self.data.len() < 4 {
            return 0;
        }
        // Number of offsets = total_offset_bytes / 4, elements = offsets - 1.
        // We need at least one offset pair to determine count.
        // First offset is at position 0; we scan until values start.
        // But simpler: the first 4 bytes of values portion should equal
        // the first offset value, which is 0. We can calculate count
        // from the total data if we know the values length, but that's
        // circular. Instead, we read offsets until we find one whose
        // byte position * 4 equals the offset value.
        //
        // Actually, for a correct encoding, offset[0] == 0. The number
        // of elements N means N+1 offsets, and offset[N] == values_len.
        // total_data = (N+1)*4 + values_len = (N+1)*4 + offset[N].
        // We need to find N. We can iterate: try N=0,1,... but that's
        // O(N). A simpler approach: read all 4-byte LE offsets starting
        // from position 0; the offset at index i gives the cumulative
        // value length. The first index where (i+1)*4 + offset[i] == data.len()
        // gives us N = i.
        //
        // For simplicity, just binary search or linear scan:
        let data = &self.data;
        let mut n = 0usize;
        loop {
            let pos = (n + 1) * 4;
            if pos + 4 > data.len() {
                break;
            }
            let off = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            if pos + 4 + off as usize == data.len() {
                return n + 1;
            }
            n += 1;
        }
        0
    }

    /// Returns `true` if the array is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the `i`-th value as a byte slice, or `None` if out of bounds.
    pub fn value(&self, i: usize) -> Option<&[u8]> {
        let off_start = i * 4;
        let off_end = (i + 1) * 4;
        if off_end + 4 > self.data.len() {
            return None;
        }
        let start = u32::from_le_bytes([
            self.data[off_start],
            self.data[off_start + 1],
            self.data[off_start + 2],
            self.data[off_start + 3],
        ]) as usize;
        let end = u32::from_le_bytes([
            self.data[off_end],
            self.data[off_end + 1],
            self.data[off_end + 2],
            self.data[off_end + 3],
        ]) as usize;
        let n_offsets = self.len() + 1;
        let values_base = n_offsets * 4;
        Some(&self.data[values_base + start..values_base + end])
    }
}

impl ArrayData for BinaryArray {
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

// ── StringArray ─────────────────────────────────────────────────────

/// A variable-length UTF-8 string array.
///
/// Wraps a [`BinaryArray`] with the [`DType::String`] dtype and adds
/// string-specific accessors.
pub struct StringArray {
    inner: BinaryArray,
}

impl StringArray {
    /// Creates a new `StringArray` from a list of string slices.
    pub fn from_slices(values: &[&str]) -> Self {
        let byte_slices: Vec<&[u8]> = values.iter().map(|s| s.as_bytes()).collect();
        let mut inner = BinaryArray::from_slices(&byte_slices);
        inner.dtype = DType::String;
        Self { inner }
    }

    /// Creates a `StringArray` from raw bytes (offsets + values already encoded).
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            inner: BinaryArray {
                dtype: DType::String,
                data: bytes,
            },
        }
    }

    /// Returns the number of elements.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the array is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the `i`-th string, or `None` if out of bounds.
    pub fn value(&self, i: usize) -> Option<&str> {
        self.inner
            .value(i)
            .map(|b| std::str::from_utf8(b).expect("invalid UTF-8 in StringArray"))
    }
}

impl ArrayData for StringArray {
    fn dtype(&self) -> &DType {
        &DType::String
    }

    fn as_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }
}

// ── Dynamic dispatch helpers ────────────────────────────────────────

/// Reconstructs a `Box<dyn ArrayData>` from raw bytes and a [`DType`].
///
/// For fixed-width types, produces a [`PrimitiveArray`]. For
/// [`DType::String`] and [`DType::Binary`], produces [`StringArray`]
/// and [`BinaryArray`] respectively.
pub fn from_bytes_dynamic(dtype: &DType, bytes: Vec<u8>) -> Result<Box<dyn ArrayData>> {
    match dtype {
        DType::UInt8 => Ok(Box::new(PrimitiveArray::<u8>::from_bytes(bytes)?)),
        DType::UInt16 => Ok(Box::new(PrimitiveArray::<u16>::from_bytes(bytes)?)),
        DType::UInt32 => Ok(Box::new(PrimitiveArray::<u32>::from_bytes(bytes)?)),
        DType::UInt64 => Ok(Box::new(PrimitiveArray::<u64>::from_bytes(bytes)?)),
        DType::Int8 => Ok(Box::new(PrimitiveArray::<i8>::from_bytes(bytes)?)),
        DType::Int16 => Ok(Box::new(PrimitiveArray::<i16>::from_bytes(bytes)?)),
        DType::Int32 => Ok(Box::new(PrimitiveArray::<i32>::from_bytes(bytes)?)),
        DType::Int64 => Ok(Box::new(PrimitiveArray::<i64>::from_bytes(bytes)?)),
        DType::Float32 => Ok(Box::new(PrimitiveArray::<f32>::from_bytes(bytes)?)),
        DType::Float64 => Ok(Box::new(PrimitiveArray::<f64>::from_bytes(bytes)?)),
        DType::String => Ok(Box::new(StringArray::from_bytes(bytes))),
        DType::Binary => Ok(Box::new(BinaryArray::from_bytes(bytes))),
        DType::Bool => Ok(Box::new(PrimitiveArray::<u8>::from_bytes(bytes)?)),
        _ => Err(Error::InvalidFooter(format!(
            "unsupported dtype for dynamic array: {dtype:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primitive_array_roundtrip() {
        let values: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0];
        let arr = PrimitiveArray::<f32>::from_slice(&values);
        assert_eq!(arr.len(), 4);
        assert_eq!(arr.values(), &values[..]);
        assert_eq!(*arr.dtype(), DType::Float32);
    }

    #[test]
    fn primitive_array_from_bytes() {
        let values: Vec<i32> = vec![10, 20, 30];
        let bytes: Vec<u8> = values
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let arr = PrimitiveArray::<i32>::from_bytes(bytes).unwrap();
        assert_eq!(arr.values(), &[10i32, 20, 30]);
    }

    #[test]
    fn primitive_array_bad_alignment() {
        let bytes = vec![0u8; 5]; // not a multiple of 4 for i32
        assert!(PrimitiveArray::<i32>::from_bytes(bytes).is_err());
    }

    #[test]
    fn string_array_roundtrip() {
        let arr = StringArray::from_slices(&["hello", "world", "!"]);
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.value(0), Some("hello"));
        assert_eq!(arr.value(1), Some("world"));
        assert_eq!(arr.value(2), Some("!"));
        assert_eq!(*ArrayData::dtype(&arr), DType::String);
    }

    #[test]
    fn binary_array_roundtrip() {
        let arr = BinaryArray::from_slices(&[b"abc", b"de", b"f"]);
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.value(0), Some(b"abc".as_slice()));
        assert_eq!(arr.value(1), Some(b"de".as_slice()));
        assert_eq!(arr.value(2), Some(b"f".as_slice()));
    }

    #[test]
    fn from_bytes_dynamic_primitive() {
        let values: Vec<u16> = vec![100, 200, 300];
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        let arr = from_bytes_dynamic(&DType::UInt16, bytes).unwrap();
        assert_eq!(*arr.dtype(), DType::UInt16);
        assert_eq!(arr.as_bytes().len(), 6);
    }

    #[test]
    fn array_data_is_object_safe() {
        let arr = PrimitiveArray::<u8>::from_slice(&[1, 2, 3]);
        let dyn_arr: &dyn ArrayData = &arr;
        assert_eq!(*dyn_arr.dtype(), DType::UInt8);
        assert_eq!(dyn_arr.as_bytes(), &[1, 2, 3]);
    }
}
