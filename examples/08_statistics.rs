//! Per-array statistics: min, max, null_count, row_count.
//!
//! Statistics are computed automatically on every flush and compact, stored in a
//! `{stem}.stats` sidecar file, and loaded eagerly on open. Callers can inspect
//! them without reading any chunk data.
//!
//! ```sh
//! cargo run --example 08_statistics
//! ```

use ndarray::Array;
use array_format::{ArrayFile, FillValue, FileConfig, InMemoryStorage, NoCompression, StatValue};

#[tokio::main]
async fn main() {
    let mut file = ArrayFile::create_memory(FileConfig::new(NoCompression)).await.unwrap();

    // Define a sensor array where -999 signals a missing reading.
    file.define_array::<i32>(
        "sensor",
        vec!["time".into()],
        vec![8],
        Some(vec![4]),
        Some(FillValue::Int(-999)),
    )
    .unwrap();

    // First chunk: four readings, one missing.
    file.write_array(
        "sensor",
        vec![0],
        Array::from_vec(vec![23i32, -999, 31, 28]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush_memory(&InMemoryStorage::new()).await.unwrap();

    let s = file.array_stats("sensor").unwrap();
    println!("After first flush:");
    println!("  min        = {:?}", s.min);      // Int(23) — fill value excluded from range
    println!("  max        = {:?}", s.max);      // Int(31)
    println!("  null_count = {}", s.null_count); // 5: 1 fill match + 4 unwritten elements
    println!("  row_count  = {}", s.row_count);  // 8: total array capacity
    assert_eq!(s.null_count, 5);
    assert_eq!(s.row_count, 8);

    // Second chunk: four more readings, none missing.
    file.write_array(
        "sensor",
        vec![4],
        Array::from_vec(vec![19i32, 25, 30, 27]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush_memory(&InMemoryStorage::new()).await.unwrap();

    let s = file.array_stats("sensor").unwrap();
    println!("\nAfter second flush (all 8 elements written):");
    println!("  min        = {:?}", s.min);      // Int(19)
    println!("  max        = {:?}", s.max);      // Int(31)
    println!("  null_count = {}", s.null_count); // 1: just the -999 fill match
    println!("  row_count  = {}", s.row_count);  // 8

    // Overwrite the first chunk with clean data — the missing value is gone.
    file.write_array(
        "sensor",
        vec![0],
        Array::from_vec(vec![23i32, 20, 31, 28]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush_memory(&InMemoryStorage::new()).await.unwrap();

    let s = file.array_stats("sensor").unwrap();
    println!("\nAfter overwriting chunk 0 (no more missing values):");
    println!("  null_count = {}", s.null_count); // 0
    assert_eq!(s.null_count, 0);

    // compact() recomputes stats from scratch.
    file.compact().await.unwrap();
    let s = file.array_stats("sensor").unwrap();
    println!("\nAfter compact:");
    println!("  min        = {:?}", s.min);
    println!("  max        = {:?}", s.max);
    println!("  row_count  = {}", s.row_count);
    assert_eq!(s.row_count, 8);

    // Stats can be used for predicate pushdown without reading chunk data.
    let query_min = 30i64;
    let has_values_above = matches!(&s.max, Some(StatValue::Int(v)) if *v >= query_min);
    println!("\nQuery: any value >= {query_min}? {has_values_above}");
    assert!(has_values_above);
}
