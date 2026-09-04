//! n-dimensional helpers for the writer.
//!
//! [`write_nd`] scatters an `ndarray` view into the chunks it covers. A chunk
//! that the region only partly covers is read back from the writer's own
//! blocks, patched, and written again.

use std::ops::Range;

use crate::array::ArrayElement;
use crate::error::{Error, Result};

use super::writer::ArrayWriter;

pub(crate) fn make_si(
    ranges: &[Range<usize>],
) -> ndarray::SliceInfo<Vec<ndarray::SliceInfoElem>, ndarray::IxDyn, ndarray::IxDyn> {
    let elems: Vec<ndarray::SliceInfoElem> = ranges
        .iter()
        .map(|r| ndarray::SliceInfoElem::Slice {
            start: r.start as isize,
            end: Some(r.end as isize),
            step: 1,
        })
        .collect();
    // SAFETY: caller ensures elems.len() equals the target array's ndim.
    unsafe { ndarray::SliceInfo::new(elems).expect("ndim/slice length mismatch") }
}

pub(crate) fn iter_nd_coords(ranges: &[Range<u32>]) -> impl Iterator<Item = Vec<u32>> + '_ {
    let counts: Vec<u32> = ranges.iter().map(|r| r.end - r.start).collect();
    let total: usize = counts.iter().map(|&c| c as usize).product();
    (0..total).map(move |mut i| {
        let mut coord = vec![0u32; ranges.len()];
        for d in (0..ranges.len()).rev() {
            coord[d] = ranges[d].start + (i as u32 % counts[d]);
            i /= counts[d] as usize;
        }
        coord
    })
}

/// Writes `data` into array `name` with its origin at `offset`.
///
/// A `data` view with no elements writes nothing and returns `Ok`. The bounds
/// of the region are still checked.
pub(crate) fn write_nd<T>(
    writer: &mut ArrayWriter,
    name: &str,
    data: ndarray::ArrayView<'_, T, ndarray::IxDyn>,
    offset: &[usize],
) -> Result<()>
where
    T: ArrayElement,
{
    let schema = writer.schema(name)?;
    if schema.dtype != T::DTYPE {
        return Err(Error::DTypeMismatch {
            expected: schema.dtype,
            actual: T::DTYPE,
        });
    }
    let ndim = schema.shape.len();
    if offset.len() != ndim || data.ndim() != ndim {
        return Err(Error::InvalidFooter(format!(
            "'{name}' has {ndim} dimensions but offset has {} and data has {}",
            offset.len(),
            data.ndim()
        )));
    }

    let full_shape: Vec<usize> = schema.shape.iter().map(|&x| x as usize).collect();
    let chunk_shape: Vec<usize> = schema.chunk_shape.iter().map(|&x| x as usize).collect();

    for i in 0..ndim {
        let end = offset[i]
            .checked_add(data.shape()[i])
            .ok_or_else(|| Error::InvalidFooter(format!("offset overflow on axis {i}")))?;
        if end > full_shape[i] {
            return Err(Error::InvalidFooter(format!(
                "write region [{}, {}) exceeds array size {} on axis {i}",
                offset[i], end, full_shape[i]
            )));
        }
    }

    // An empty region covers no chunk. This happens when an axis of the
    // array has length 0, which NetCDF uses for absent records.
    if data.is_empty() {
        return Ok(());
    }

    let write_end: Vec<usize> = (0..ndim).map(|i| offset[i] + data.shape()[i]).collect();

    let chunk_ranges: Vec<Range<u32>> = (0..ndim)
        .map(|i| {
            let start = (offset[i] / chunk_shape[i]) as u32;
            let end = write_end[i]
                .div_ceil(chunk_shape[i])
                .min(full_shape[i].div_ceil(chunk_shape[i])) as u32;
            start..end
        })
        .collect();

    // Phase 1: encode every covered chunk. Reads only.
    let mut writes: Vec<(Vec<u32>, Vec<u8>)> = Vec::new();

    for coord in iter_nd_coords(&chunk_ranges) {
        let chunk_global: Vec<Range<usize>> = (0..ndim)
            .map(|i| {
                let start = coord[i] as usize * chunk_shape[i];
                let end = (start + chunk_shape[i]).min(full_shape[i]);
                start..end
            })
            .collect();

        let chunk_actual_shape: Vec<usize> = chunk_global.iter().map(|r| r.end - r.start).collect();

        let overlap: Vec<Range<usize>> = (0..ndim)
            .map(|i| offset[i].max(chunk_global[i].start)..write_end[i].min(chunk_global[i].end))
            .collect();

        if overlap.iter().any(|r| r.is_empty()) {
            continue;
        }

        let full_cover = (0..ndim).all(|i| overlap[i] == chunk_global[i]);

        let input_local: Vec<Range<usize>> = (0..ndim)
            .map(|i| (overlap[i].start - offset[i])..(overlap[i].end - offset[i]))
            .collect();

        let encoded: Vec<u8> = if full_cover {
            let v: Vec<T> = data.slice(make_si(&input_local)).iter().cloned().collect();
            T::encode_chunk(&v)
        } else {
            let base = writer.read_chunk::<T>(name, &coord)?;
            let mut chunk_nd =
                ndarray::Array::from_shape_vec(ndarray::IxDyn(&chunk_actual_shape), base)
                    .map_err(|e| Error::InvalidFooter(e.to_string()))?;

            let chunk_local: Vec<Range<usize>> = (0..ndim)
                .map(|i| {
                    (overlap[i].start - chunk_global[i].start)
                        ..(overlap[i].end - chunk_global[i].start)
                })
                .collect();

            chunk_nd
                .slice_mut(make_si(&chunk_local))
                .assign(&data.slice(make_si(&input_local)));

            let patched: Vec<T> = chunk_nd.iter().cloned().collect();
            T::encode_chunk(&patched)
        };

        writes.push((coord, encoded));
    }

    // Phase 2: store them. Writes only.
    for (coord, bytes) in writes {
        writer.write_chunk_raw(name, coord, &bytes)?;
    }

    Ok(())
}
