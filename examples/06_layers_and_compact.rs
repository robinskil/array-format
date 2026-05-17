//! Layered writes (flush) and compaction.
//!
//! Each flush creates a new overlay sidecar that shadows earlier versions of
//! changed chunks. Compact merges all layers into one.
//!
//! ```sh
//! cargo run --example 06_layers_and_compact
//! ```

use ndarray::Array;
use array_format::{File, FileConfig, InMemoryStorage, NoCompression};

#[tokio::main]
async fn main() {
    let mut file = File::create_memory(FileConfig::new(NoCompression)).await.unwrap();
    file.define_array::<i32>("values", vec!["x".into()], vec![4], None, None).unwrap();

    // Layer 1: write initial data
    let v1 = Array::from_vec(vec![1i32, 2, 3, 4]).into_dyn();
    file.write_array("values", vec![0], v1.view()).await.unwrap();
    let ov1 = InMemoryStorage::new();
    file.flush_memory(&ov1).await.unwrap();
    println!("after flush 1: {} layer(s)", file.num_layers()); // 2

    // Layer 2: patch a single element
    let patch = Array::from_vec(vec![99i32]).into_dyn();
    file.write_array("values", vec![2], patch.view()).await.unwrap();
    let ov2 = InMemoryStorage::new();
    file.flush_memory(&ov2).await.unwrap();
    println!("after flush 2: {} layer(s)", file.num_layers()); // 3

    let before = file.read_array::<i32>("values", vec![], vec![]).await.unwrap();
    println!("before compact = {:?}", before.as_slice().unwrap()); // [1, 2, 99, 4]

    // Compact: merge everything into the base layer
    file.compact().await.unwrap();
    println!("after compact:  {} layer(s)", file.num_layers()); // 1

    let after = file.read_array::<i32>("values", vec![], vec![]).await.unwrap();
    println!("after compact  = {:?}", after.as_slice().unwrap()); // [1, 2, 99, 4]
    assert_eq!(after[[2]], 99);
}
