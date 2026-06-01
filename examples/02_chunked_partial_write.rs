//! Chunked arrays with partial writes at a given offset.
//!
//! Shape [12] split into three chunks of 4.  Only some chunks are written;
//! a patch lands in the middle of a chunk → read-modify-write is done
//! automatically.
//!
//! ```sh
//! cargo run --example 02_chunked_partial_write
//! ```

use array_format::{ArrayFile, FileConfig, InMemoryStorage, NoCompression};
use ndarray::Array;

#[tokio::main]
async fn main() {
    let mut file = ArrayFile::create_memory(FileConfig::new(NoCompression))
        .await
        .unwrap();

    file.define_array::<i32>(
        "signal",
        vec!["t".into()],
        vec![12],
        Some(vec![4]), // three chunks of four elements each
        None,
    )
    .unwrap();

    // Write chunk 0 (indices 0-3) in full
    let first = Array::from_vec(vec![1i32, 2, 3, 4]).into_dyn();
    file.write_array("signal", vec![0], first.view())
        .await
        .unwrap();

    // Partial write into chunk 1: only indices 5-6
    let patch = Array::from_vec(vec![99i32, 100]).into_dyn();
    file.write_array("signal", vec![5], patch.view())
        .await
        .unwrap();

    // Chunk 2 (indices 8-11) is never written → stays at fill value (0)

    let ov = InMemoryStorage::new();
    file.flush_memory(&ov).await.unwrap();

    let full = file
        .read_array::<i32>("signal", vec![], vec![])
        .await
        .unwrap();
    println!("full signal  = {:?}", full.as_slice().unwrap());
    // chunk 0: [1, 2, 3, 4]   chunk 1: [0, 99, 100, 0]   chunk 2: [0, 0, 0, 0]

    // Read only a sub-region: indices 4..9
    let sub = file
        .read_array::<i32>("signal", vec![4], vec![5])
        .await
        .unwrap();
    println!("signal[4..9] = {:?}", sub.as_slice().unwrap());
    assert_eq!(sub.as_slice().unwrap(), [0, 99, 100, 0, 0]);
}
