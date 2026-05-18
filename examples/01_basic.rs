//! Basic array write and read.
//!
//! ```sh
//! cargo run --example 01_basic
//! ```

use ndarray::Array;
use array_format::{ArrayFile, FileConfig, InMemoryStorage, NoCompression};

#[tokio::main]
async fn main() {
    let mut file = ArrayFile::create_memory(FileConfig::new(NoCompression)).await.unwrap();

    // 1-D float array
    file.define_array::<f64>("temperature", vec!["time".into()], vec![6], None, None).unwrap();
    let temps = Array::from_vec(vec![20.1f64, 21.3, 19.8, 22.4, 18.9, 23.1]).into_dyn();
    file.write_array("temperature", vec![0], temps.view()).await.unwrap();

    // 2-D integer array
    file.define_array::<i32>("grid", vec!["x".into(), "y".into()], vec![3, 4], None, None).unwrap();
    let grid = Array::from_shape_vec(ndarray::IxDyn(&[3, 4]), (0i32..12).collect()).unwrap();
    file.write_array("grid", vec![0, 0], grid.view()).await.unwrap();

    let ov = InMemoryStorage::new();
    file.flush_memory(&ov).await.unwrap();

    let t = file.read_array::<f64>("temperature", vec![], vec![]).await.unwrap();
    let g = file.read_array::<i32>("grid", vec![], vec![]).await.unwrap();

    println!("temperature = {:?}", t.as_slice().unwrap());
    println!("grid[1, 2]  = {}", g[[1, 2]]);
    assert!((t[[2]] - 19.8).abs() < 1e-10);
    assert_eq!(g[[1, 2]], 6);
}
