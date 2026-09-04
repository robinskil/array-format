//! Changing an existing file.
//!
//! A file is immutable once finished. To change it, open it, copy the arrays
//! you keep into a new writer, patch or add what you need, and finish to a
//! new path. The original stays intact until you delete it.
//!
//! ```sh
//! cargo run --example 06_edit_a_file
//! ```

use std::sync::Arc;

use array_format::{
    ArrayFile, ArrayWriter, AttributeValue, NoCompression, ReadConfig, WriterConfig,
};
use ndarray::Array;
use object_store::{ObjectStore, memory::InMemory};

#[tokio::main]
async fn main() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let v1 = object_store::path::Path::from("data.v1.af");
    let v2 = object_store::path::Path::from("data.v2.af");

    // Version 1: two arrays.
    let mut writer = ArrayWriter::new(WriterConfig::new(NoCompression));
    writer
        .define_array::<i32>("values", vec!["x".into()], vec![4], None, None)
        .unwrap();
    writer
        .write_array(
            "values",
            vec![0],
            Array::from_vec(vec![1i32, 2, 3, 4]).into_dyn().view(),
        )
        .unwrap();
    writer
        .set_attribute("values", "units", AttributeValue::String("counts".into()))
        .unwrap();
    writer
        .define_array::<u8>("scratch", vec!["x".into()], vec![2], None, None)
        .unwrap();
    writer.finish(Arc::clone(&store), v1.clone()).await.unwrap();

    // Version 2: keep "values" but patch one element, drop "scratch", add "flags".
    let source = ArrayFile::open(Arc::clone(&store), v1.clone(), ReadConfig::default())
        .await
        .unwrap();
    let mut writer = ArrayWriter::new(WriterConfig::new(NoCompression));
    writer.copy_array(&source, "values").await.unwrap(); // definition, chunks, attributes
    writer
        .write_array(
            "values",
            vec![2],
            Array::from_vec(vec![99i32]).into_dyn().view(),
        )
        .unwrap();
    writer
        .define_array::<u8>("flags", vec!["x".into()], vec![4], None, None)
        .unwrap();
    writer
        .write_array(
            "flags",
            vec![0],
            Array::from_vec(vec![1u8, 0, 1, 0]).into_dyn().view(),
        )
        .unwrap();
    let edited = writer.finish(Arc::clone(&store), v2).await.unwrap();

    let names = |file: &ArrayFile| -> Vec<String> {
        file.arrays().iter().map(|a| a.name.to_string()).collect()
    };
    println!("v1 arrays: {:?}", names(&source));
    println!("v2 arrays: {:?}", names(&edited));

    let values = edited
        .read_array::<i32>("values", vec![], vec![])
        .await
        .unwrap();
    println!("v2 values  = {:?}", values.as_slice().unwrap()); // [1, 2, 99, 4]
    println!("v2 units   = {:?}", edited.get_attribute("values", "units"));

    assert_eq!(values[[2]], 99);
    assert_eq!(
        edited.get_attribute("values", "units"),
        Some(&AttributeValue::String("counts".into()))
    );
    assert!(edited.array("scratch").is_none());
    assert!(source.array("scratch").is_some(), "v1 is untouched");
}
