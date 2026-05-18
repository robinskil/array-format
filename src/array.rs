use crate::dtype::DType;
use crate::layout::FillValue;

// ── ArrayElement trait ──────────────────────────────────────────────

/// Unified element type for all array operations.
///
/// Implemented by all fixed-width numeric primitives and variable-length
/// types (`String`, `Vec<u8>`). The trait provides chunk-level encode/decode
/// and fill-value generation so that a single generic code path handles both.
pub trait ArrayElement: Clone + Send + Sync + 'static {
    const DTYPE: DType;
    fn encode_chunk(values: &[Self]) -> Vec<u8>;
    fn decode_chunk(bytes: &[u8]) -> Vec<Self>;
    fn fill_element(fill: Option<&FillValue>) -> Self;
}

// ── Shared helpers for fixed-width types ─────────────────────────────

fn encode_copy<T: Sized>(values: &[T]) -> Vec<u8> {
    let byte_len = std::mem::size_of_val(values);
    unsafe { std::slice::from_raw_parts(values.as_ptr() as *const u8, byte_len) }.to_vec()
}

fn decode_copy<T: Sized>(bytes: &[u8]) -> Vec<T> {
    let elem = std::mem::size_of::<T>();
    if bytes.is_empty() || elem == 0 {
        return vec![];
    }
    let n = bytes.len() / elem;
    let mut out: Vec<T> = Vec::with_capacity(n);
    // SAFETY: numeric primitives have no invalid bit patterns; Vec<T> is aligned.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), out.as_mut_ptr() as *mut u8, n * elem);
        out.set_len(n);
    }
    out
}

// ── Numeric implementations ──────────────────────────────────────────

macro_rules! impl_element_uint {
    ($ty:ty, $variant:expr) => {
        impl ArrayElement for $ty {
            const DTYPE: DType = $variant;
            fn encode_chunk(values: &[Self]) -> Vec<u8> {
                encode_copy(values)
            }
            fn decode_chunk(bytes: &[u8]) -> Vec<Self> {
                decode_copy(bytes)
            }
            fn fill_element(fill: Option<&FillValue>) -> Self {
                match fill {
                    Some(FillValue::UInt(v)) => *v as $ty,
                    Some(FillValue::Int(v)) => *v as $ty,
                    Some(FillValue::Float(v)) => *v as $ty,
                    Some(FillValue::Bool(v)) => *v as u8 as $ty,
                    _ => 0,
                }
            }
        }
    };
}

macro_rules! impl_element_int {
    ($ty:ty, $variant:expr) => {
        impl ArrayElement for $ty {
            const DTYPE: DType = $variant;
            fn encode_chunk(values: &[Self]) -> Vec<u8> {
                encode_copy(values)
            }
            fn decode_chunk(bytes: &[u8]) -> Vec<Self> {
                decode_copy(bytes)
            }
            fn fill_element(fill: Option<&FillValue>) -> Self {
                match fill {
                    Some(FillValue::Int(v)) => *v as $ty,
                    Some(FillValue::UInt(v)) => *v as $ty,
                    Some(FillValue::Float(v)) => *v as $ty,
                    _ => 0,
                }
            }
        }
    };
}

macro_rules! impl_element_float {
    ($ty:ty, $variant:expr) => {
        impl ArrayElement for $ty {
            const DTYPE: DType = $variant;
            fn encode_chunk(values: &[Self]) -> Vec<u8> {
                encode_copy(values)
            }
            fn decode_chunk(bytes: &[u8]) -> Vec<Self> {
                decode_copy(bytes)
            }
            fn fill_element(fill: Option<&FillValue>) -> Self {
                match fill {
                    Some(FillValue::Float(v)) => *v as $ty,
                    Some(FillValue::Int(v)) => *v as $ty,
                    Some(FillValue::UInt(v)) => *v as $ty,
                    _ => 0.0,
                }
            }
        }
    };
}

impl_element_uint!(u8, DType::UInt8);
impl_element_uint!(u16, DType::UInt16);
impl_element_uint!(u32, DType::UInt32);
impl_element_uint!(u64, DType::UInt64);
impl_element_int!(i8, DType::Int8);
impl_element_int!(i16, DType::Int16);
impl_element_int!(i32, DType::Int32);
impl_element_int!(i64, DType::Int64);
impl_element_float!(f32, DType::Float32);
impl_element_float!(f64, DType::Float64);

// ── Variable-length helpers ──────────────────────────────────────────

/// Encodes a sequence of byte slices into the offset-buffer format:
/// `[N+1 u32 LE offsets][concatenated values]`.
fn encode_offsets<'a>(slices: impl Iterator<Item = &'a [u8]>) -> Vec<u8> {
    let slices: Vec<&[u8]> = slices.collect();
    let mut offsets: Vec<u32> = Vec::with_capacity(slices.len() + 1);
    let mut buf: Vec<u8> = Vec::new();
    offsets.push(0);
    for s in &slices {
        buf.extend_from_slice(s);
        offsets.push(buf.len() as u32);
    }
    let mut data = Vec::with_capacity(offsets.len() * 4 + buf.len());
    for off in &offsets {
        data.extend_from_slice(&off.to_le_bytes());
    }
    data.extend_from_slice(&buf);
    data
}

/// Decodes an offset-buffer chunk back to a `Vec<Vec<u8>>`.
fn decode_offsets(bytes: &[u8]) -> Vec<Vec<u8>> {
    let n = {
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
    };
    if n == 0 {
        return vec![];
    }
    let values_base = (n + 1) * 4;
    (0..n)
        .map(|i| {
            let start = u32::from_le_bytes(bytes[i * 4..i * 4 + 4].try_into().unwrap()) as usize;
            let end = u32::from_le_bytes(bytes[(i + 1) * 4..(i + 1) * 4 + 4].try_into().unwrap())
                as usize;
            bytes[values_base + start..values_base + end].to_vec()
        })
        .collect()
}

// ── String ───────────────────────────────────────────────────────────

impl ArrayElement for String {
    const DTYPE: DType = DType::String;

    fn encode_chunk(values: &[Self]) -> Vec<u8> {
        encode_offsets(values.iter().map(|s| s.as_bytes()))
    }

    fn decode_chunk(bytes: &[u8]) -> Vec<Self> {
        decode_offsets(bytes)
            .into_iter()
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .collect()
    }

    fn fill_element(_fill: Option<&FillValue>) -> Self {
        String::new()
    }
}

// ── Vec<u8> ──────────────────────────────────────────────────────────

impl ArrayElement for Vec<u8> {
    const DTYPE: DType = DType::Binary;

    fn encode_chunk(values: &[Self]) -> Vec<u8> {
        encode_offsets(values.iter().map(|v| v.as_slice()))
    }

    fn decode_chunk(bytes: &[u8]) -> Vec<Self> {
        decode_offsets(bytes)
    }

    fn fill_element(_fill: Option<&FillValue>) -> Self {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_roundtrip_f32() {
        let values = vec![1.0f32, 2.5, 3.14];
        let bytes = f32::encode_chunk(&values);
        assert_eq!(bytes.len(), 12);
        let back = f32::decode_chunk(&bytes);
        assert_eq!(back, values);
    }

    #[test]
    fn numeric_roundtrip_i32() {
        let values = vec![-1i32, 0, 42, i32::MAX];
        let bytes = i32::encode_chunk(&values);
        let back = i32::decode_chunk(&bytes);
        assert_eq!(back, values);
    }

    #[test]
    fn string_roundtrip() {
        let values = vec!["hello".to_string(), "".to_string(), "world!".to_string()];
        let bytes = String::encode_chunk(&values);
        let back = String::decode_chunk(&bytes);
        assert_eq!(back, values);
    }

    #[test]
    fn binary_roundtrip() {
        let values = vec![vec![1u8, 2, 3], vec![], vec![255]];
        let bytes = Vec::<u8>::encode_chunk(&values);
        let back = Vec::<u8>::decode_chunk(&bytes);
        assert_eq!(back, values);
    }

    #[test]
    fn fill_element_numeric() {
        assert_eq!(i32::fill_element(Some(&FillValue::Int(-7))), -7i32);
        assert_eq!(f64::fill_element(Some(&FillValue::Float(3.14))), 3.14f64);
        assert_eq!(u8::fill_element(None), 0u8);
    }

    #[test]
    fn fill_element_vlen_ignores_fill() {
        assert_eq!(String::fill_element(Some(&FillValue::Int(99))), "");
        assert_eq!(Vec::<u8>::fill_element(None), Vec::<u8>::new());
    }

    #[test]
    fn decode_empty() {
        assert_eq!(i32::decode_chunk(&[]), Vec::<i32>::new());
        assert_eq!(String::decode_chunk(&[]), Vec::<String>::new());
    }
}
