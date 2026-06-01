//! Internal n-dimensional read/write helpers built on the `ndarray` crate.
//!
//! These are `pub(crate)` building blocks used by [`ArrayFile`](crate::ArrayFile)
//! to translate between coordinate-addressed chunks and `ndarray` views:
//! [`write_nd`] scatters an array into its covering chunks, [`assemble_nd`]
//! gathers a (possibly sliced) array back out, and [`make_si`]/[`iter_nd_coords`]
//! handle slice-info construction and chunk-coordinate iteration.

use std::ops::Range;

use crate::array::ArrayElement;
use crate::error::{Error, Result};
use crate::file::{ArrayFile, ChunkedSchema};

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

/// Assembles an array from `file`, reading only chunks that overlap `slice`
/// (or the full array when `slice` is `None`).
pub(crate) async fn assemble_nd<T>(
    file: &ArrayFile,
    name: &str,
    slice: Option<&[Range<usize>]>,
) -> Result<ndarray::ArcArray<T, ndarray::IxDyn>>
where
    T: ArrayElement,
{
    let ChunkedSchema {
        full_shape: full_shape_u32,
        chunk_shape: chunk_shape_u32,
        dtype,
        all_coords,
    } = file.get_chunked_schema(name)?;
    if dtype != T::DTYPE {
        return Err(Error::DTypeMismatch {
            expected: dtype,
            actual: T::DTYPE,
        });
    }
    let fill = T::fill_element(file.get_array(name)?.fill_value.as_ref());
    let full_shape: Vec<usize> = full_shape_u32.iter().map(|&x| x as usize).collect();
    let chunk_shape: Vec<usize> = chunk_shape_u32.iter().map(|&x| x as usize).collect();
    let ndim = full_shape.len();

    let effective: Vec<Range<usize>> = match slice {
        None => full_shape.iter().map(|&s| 0..s).collect(),
        Some(s) => {
            if s.len() != ndim {
                return Err(Error::InvalidFooter(format!(
                    "slice has {} axes but '{name}' has {ndim}",
                    s.len()
                )));
            }
            s.iter()
                .zip(&full_shape)
                .map(|(r, &s)| r.start.min(s)..r.end.min(s))
                .collect()
        }
    };

    let output_shape: Vec<usize> = effective.iter().map(|r| r.end - r.start).collect();
    let mut output =
        ndarray::Array::<T, ndarray::IxDyn>::from_elem(ndarray::IxDyn(&output_shape), fill);

    for coord in all_coords {
        let chunk_range: Vec<Range<usize>> = (0..ndim)
            .map(|i| {
                let start = coord[i] as usize * chunk_shape[i];
                let end = (start + chunk_shape[i]).min(full_shape[i]);
                start..end
            })
            .collect();

        let overlap: Vec<Range<usize>> = (0..ndim)
            .map(|i| {
                effective[i].start.max(chunk_range[i].start)
                    ..effective[i].end.min(chunk_range[i].end)
            })
            .collect();

        if overlap.iter().any(|r| r.is_empty()) {
            continue;
        }

        let values = file.read_chunk::<T>(name, &coord).await?;
        let chunk_actual_shape: Vec<usize> = chunk_range.iter().map(|r| r.end - r.start).collect();

        let chunk_nd = ndarray::Array::from_shape_vec(ndarray::IxDyn(&chunk_actual_shape), values)
            .map_err(|e| Error::InvalidFooter(e.to_string()))?;

        let chunk_si = make_si(
            &(0..ndim)
                .map(|i| {
                    (overlap[i].start - chunk_range[i].start)
                        ..(overlap[i].end - chunk_range[i].start)
                })
                .collect::<Vec<_>>(),
        );

        let out_si = make_si(
            &(0..ndim)
                .map(|i| {
                    (overlap[i].start - effective[i].start)..(overlap[i].end - effective[i].start)
                })
                .collect::<Vec<_>>(),
        );

        output.slice_mut(out_si).assign(&chunk_nd.slice(chunk_si));
    }

    Ok(output.into_shared())
}

/// Writes `data` into a chunked array at `offset`, performing
/// read-modify-write for partial chunks.
pub(crate) async fn write_nd<T>(
    file: &mut ArrayFile,
    name: &str,
    data: ndarray::ArrayView<'_, T, ndarray::IxDyn>,
    offset: &[usize],
) -> Result<()>
where
    T: ArrayElement,
{
    let ChunkedSchema {
        full_shape: full_shape_u32,
        chunk_shape: chunk_shape_u32,
        dtype,
        ..
    } = file.get_chunked_schema(name)?;

    if dtype != T::DTYPE {
        return Err(Error::DTypeMismatch {
            expected: dtype,
            actual: T::DTYPE,
        });
    }
    let ndim = full_shape_u32.len();
    if offset.len() != ndim || data.ndim() != ndim {
        return Err(Error::InvalidFooter(format!(
            "'{name}' has {ndim} dimensions but offset has {} and data has {}",
            offset.len(),
            data.ndim()
        )));
    }

    let full_shape: Vec<usize> = full_shape_u32.iter().map(|&x| x as usize).collect();
    let chunk_shape: Vec<usize> = chunk_shape_u32.iter().map(|&x| x as usize).collect();

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

    // Phase 1: collect (coord, encoded_bytes) — reads allowed.
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
            let mut base = file.read_chunk::<T>(name, &coord).await?;
            let mut chunk_nd =
                ndarray::Array::from_shape_vec(ndarray::IxDyn(&chunk_actual_shape), base.clone())
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

            base = chunk_nd.iter().cloned().collect();
            T::encode_chunk(&base)
        };

        writes.push((coord, encoded));
    }

    // Phase 2: apply writes — mutable, no reads.
    for (coord, bytes) in writes {
        file.write_chunk_raw(name, coord, bytes)?;
    }

    Ok(())
}
