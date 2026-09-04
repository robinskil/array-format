//! Chunked arrays with partial writes at a given offset.
//!
//! Shape [12] split into three chunks of 4. Only some chunks are written; a
//! patch lands in the middle of a chunk, so the writer reads that chunk back,
//! patches it and writes it again.
//!
//! ```sh
//! cargo run --example 02_chunked_partial_write
//! ```

use std::sync::Arc;

use array_format::{ArrayWriter, NoCompression, WriterConfig};
use ndarray::Array;
use object_store::memory::InMemory;

#[tokio::main]
async fn main() {
    let mut writer = ArrayWriter::new(WriterConfig::new(NoCompression));

    writer
        .define_array::<i32>(
            "signal",
            vec!["t".into()],
            vec![12],
            Some(vec![4]), // three chunks of four elements each
            None,
        )
        .unwrap();

    // Write chunk 0 (indices 0-3) in full
    let first = Array::from_vec(vec![1i32, 2, 3, 4]).into_dyn();
    writer.write_array("signal", vec![0], first.view()).unwrap();

    // Partial write into chunk 1: only indices 5-6
    let patch = Array::from_vec(vec![99i32, 100]).into_dyn();
    writer.write_array("signal", vec![5], patch.view()).unwrap();

    // Chunk 2 (indices 8-11) is never written and reads as the fill value (0)

    let file = writer
        .finish(
            Arc::new(InMemory::new()),
            object_store::path::Path::from("signal.af"),
        )
        .await
        .unwrap();

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
