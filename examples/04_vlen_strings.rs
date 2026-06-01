//! Variable-length string arrays.
//!
//! ```sh
//! cargo run --example 04_vlen_strings
//! ```

use array_format::{ArrayFile, FileConfig, NoCompression};

#[tokio::main]
async fn main() {
    let mut file = ArrayFile::create_memory(FileConfig::new(NoCompression))
        .await
        .unwrap();

    file.define_array::<String>("labels", vec!["i".into()], vec![4], None, None)
        .unwrap();
    let labels = ndarray::arr1(&[
        "alpha".to_string(),
        "beta".to_string(),
        "gamma".to_string(),
        "delta".to_string(),
    ])
    .into_dyn();
    file.write_array("labels", vec![0], labels.view())
        .await
        .unwrap();

    file.flush().await.unwrap();

    let out = file
        .read_array::<String>("labels", vec![], vec![])
        .await
        .unwrap();
    println!("labels = {:?}", out.as_slice().unwrap());
    assert_eq!(out[[2]], "gamma");
}
