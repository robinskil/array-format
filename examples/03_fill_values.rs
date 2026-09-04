//! Per-array fill values for unwritten or missing chunks.
//!
//! ```sh
//! cargo run --example 03_fill_values
//! ```

use std::sync::Arc;

use array_format::{ArrayWriter, FillValue, NoCompression, WriterConfig};
use ndarray::Array;
use object_store::memory::InMemory;

#[tokio::main]
async fn main() {
    let mut writer = ArrayWriter::new(WriterConfig::new(NoCompression));

    // Sensor array: -999 signals "no data"
    writer
        .define_array::<i32>(
            "sensor",
            vec!["x".into()],
            vec![8],
            Some(vec![4]),
            Some(FillValue::Int(-999)),
        )
        .unwrap();

    // Write only the first four elements; the second chunk is left unwritten.
    let data = Array::from_vec(vec![10i32, 20, 30, 40]).into_dyn();
    writer.write_array("sensor", vec![0], data.view()).unwrap();

    let file = writer
        .finish(
            Arc::new(InMemory::new()),
            object_store::path::Path::from("sensor.af"),
        )
        .await
        .unwrap();

    let out = file
        .read_array::<i32>("sensor", vec![], vec![])
        .await
        .unwrap();
    println!("sensor = {:?}", out.as_slice().unwrap());
    // → [10, 20, 30, 40, -999, -999, -999, -999]
    assert_eq!(out.as_slice().unwrap()[4..], [-999, -999, -999, -999]);
}
