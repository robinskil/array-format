//! Per-array fill values for unwritten or missing chunks.
//!
//! ```sh
//! cargo run --example 03_fill_values
//! ```

use array_format::{ArrayFile, FileConfig, FillValue, NoCompression};
use ndarray::Array;

#[tokio::main]
async fn main() {
    let mut file = ArrayFile::create_memory(FileConfig::new(NoCompression))
        .await
        .unwrap();

    // Sensor array: -999 signals "no data"
    file.define_array::<i32>(
        "sensor",
        vec!["x".into()],
        vec![8],
        Some(vec![4]),
        Some(FillValue::Int(-999)),
    )
    .unwrap();

    // Write only the first four elements; the second chunk is left unwritten.
    let data = Array::from_vec(vec![10i32, 20, 30, 40]).into_dyn();
    file.write_array("sensor", vec![0], data.view())
        .await
        .unwrap();

    file.flush().await.unwrap();

    let out = file
        .read_array::<i32>("sensor", vec![], vec![])
        .await
        .unwrap();
    println!("sensor = {:?}", out.as_slice().unwrap());
    // → [10, 20, 30, 40, -999, -999, -999, -999]
    assert_eq!(out.as_slice().unwrap()[4..], [-999, -999, -999, -999]);
}
