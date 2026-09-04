//! Variable-length string arrays.
//!
//! ```sh
//! cargo run --example 04_vlen_strings
//! ```

use std::sync::Arc;

use array_format::{ArrayWriter, NoCompression, WriterConfig};
use object_store::memory::InMemory;

#[tokio::main]
async fn main() {
    let mut writer = ArrayWriter::new(WriterConfig::new(NoCompression));

    writer
        .define_array::<String>("labels", vec!["i".into()], vec![4], None, None)
        .unwrap();
    let labels = ndarray::arr1(&[
        "alpha".to_string(),
        "beta".to_string(),
        "gamma".to_string(),
        "delta".to_string(),
    ])
    .into_dyn();
    writer
        .write_array("labels", vec![0], labels.view())
        .unwrap();

    let file = writer
        .finish(
            Arc::new(InMemory::new()),
            object_store::path::Path::from("labels.af"),
        )
        .await
        .unwrap();

    let out = file
        .read_array::<String>("labels", vec![], vec![])
        .await
        .unwrap();
    println!("labels = {:?}", out.as_slice().unwrap());
    assert_eq!(out[[2]], "gamma");
}
