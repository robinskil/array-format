//! Per-array statistics: min, max, null_count, row_count.
//!
//! The writer computes a partial per chunk as it is written and stores the
//! merged result in the footer, so statistics are available the moment a file
//! opens, without reading any chunk data.
//!
//! ```sh
//! cargo run --example 08_statistics
//! ```

use std::sync::Arc;

use array_format::{ArrayWriter, FillValue, NoCompression, StatValue, WriterConfig};
use ndarray::Array;
use object_store::{ObjectStore, memory::InMemory};

fn sensor_writer() -> ArrayWriter {
    let mut writer = ArrayWriter::new(WriterConfig::new(NoCompression));
    // -999 signals a missing reading.
    writer
        .define_array::<i32>(
            "sensor",
            vec!["time".into()],
            vec![8],
            Some(vec![4]),
            Some(FillValue::Int(-999)),
        )
        .unwrap();
    writer
}

#[tokio::main]
async fn main() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // A file with only the first chunk written: four readings, one missing.
    let mut writer = sensor_writer();
    writer
        .write_array(
            "sensor",
            vec![0],
            Array::from_vec(vec![23i32, -999, 31, 28]).into_dyn().view(),
        )
        .unwrap();
    let partial = writer
        .finish(
            Arc::clone(&store),
            object_store::path::Path::from("partial.af"),
        )
        .await
        .unwrap();

    let s = partial.array_stats("sensor").unwrap();
    println!("One chunk written:");
    println!("  min        = {:?}", s.min); // Int(23) — fill value excluded from range
    println!("  max        = {:?}", s.max); // Int(31)
    println!("  null_count = {}", s.null_count); // 5: 1 fill match + 4 unwritten elements
    println!("  row_count  = {}", s.row_count); // 8: total array capacity
    assert_eq!(s.null_count, 5);
    assert_eq!(s.row_count, 8);

    // A file with both chunks, where chunk 0 was rewritten with clean data
    // before finishing. Only the final content counts.
    let mut writer = sensor_writer();
    writer
        .write_array(
            "sensor",
            vec![0],
            Array::from_vec(vec![23i32, -999, 31, 28]).into_dyn().view(),
        )
        .unwrap();
    writer
        .write_array(
            "sensor",
            vec![4],
            Array::from_vec(vec![19i32, 25, 30, 27]).into_dyn().view(),
        )
        .unwrap();
    writer
        .write_array(
            "sensor",
            vec![0],
            Array::from_vec(vec![23i32, 20, 31, 28]).into_dyn().view(),
        )
        .unwrap();
    let full = writer
        .finish(
            Arc::clone(&store),
            object_store::path::Path::from("full.af"),
        )
        .await
        .unwrap();

    let s = full.array_stats("sensor").unwrap();
    println!("\nAll eight elements written, chunk 0 rewritten:");
    println!("  min        = {:?}", s.min); // Int(19)
    println!("  max        = {:?}", s.max); // Int(31)
    println!("  null_count = {}", s.null_count); // 0
    println!("  row_count  = {}", s.row_count); // 8
    assert_eq!(s.null_count, 0);
    assert_eq!(s.row_count, 8);

    // Stats can be used for predicate pushdown without reading chunk data.
    let query_min = 30i64;
    let has_values_above = matches!(&s.max, Some(StatValue::Int(v)) if *v >= query_min);
    println!("\nQuery: any value >= {query_min}? {has_values_above}");
    assert!(has_values_above);

    // Every array's stats, in file order, in one pass.
    for stats in full.stats() {
        println!("{}: {} rows", stats.name, stats.row_count);
    }
}
