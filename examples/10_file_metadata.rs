//! File-level metadata via a placeholder array.
//!
//! Attributes attach to arrays, so to store metadata about the *file* as a
//! whole (title, provenance, schema version, …) without any real data, define a
//! scalar placeholder array with an empty shape and hang the attributes on it.
//! Nothing is written to it, so it costs its attributes and a few bytes of
//! metadata.
//!
//! ```sh
//! cargo run --example 10_file_metadata
//! ```

use std::sync::Arc;

use array_format::{ArrayWriter, AttributeValue, NoCompression, WriterConfig};
use object_store::memory::InMemory;

/// Reserved array name used to hold file-level metadata.
const FILE_META: &str = "__file__";

#[tokio::main]
async fn main() {
    let mut writer = ArrayWriter::new(WriterConfig::new(NoCompression));

    // A scalar placeholder: empty shape means no dimensions, and it is never
    // written, so it has no chunks.
    writer
        .define_array::<u8>(FILE_META, vec![], vec![], None, None)
        .unwrap();

    writer
        .set_attribute(
            FILE_META,
            "title",
            AttributeValue::String("My Dataset".into()),
        )
        .unwrap();
    writer
        .set_attribute(FILE_META, "schema_version", AttributeValue::Int32(3))
        .unwrap();
    writer
        .set_attribute(
            FILE_META,
            "authors",
            AttributeValue::StringList(Box::new(["alice".into(), "bob".into()])),
        )
        .unwrap();

    // A metadata-only file is completely valid.
    let file = writer
        .finish(
            Arc::new(InMemory::new()),
            object_store::path::Path::from("meta.af"),
        )
        .await
        .unwrap();

    // Read the file-level metadata back.
    let title = file.get_attribute(FILE_META, "title");
    let version = file.get_attribute(FILE_META, "schema_version");
    let authors = file.get_attribute(FILE_META, "authors");
    println!("title          = {title:?}");
    println!("schema_version = {version:?}");
    println!("authors        = {authors:?}");

    // The placeholder is listed like any array; filter it out to present only
    // real data arrays to users.
    let data_arrays: Vec<&str> = file
        .arrays()
        .iter()
        .map(|info| info.name.as_str())
        .filter(|name| *name != FILE_META)
        .collect();
    println!("data arrays    = {data_arrays:?}"); // [] — metadata only

    assert_eq!(title, Some(&AttributeValue::String("My Dataset".into())));
    assert!(data_arrays.is_empty());
}
