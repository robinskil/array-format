//! File-level metadata via a placeholder array.
//!
//! Attributes attach to arrays, so to store metadata about the *file* as a
//! whole (title, provenance, schema version, …) without any real data, define a
//! scalar placeholder array with an empty shape and hang the attributes on it.
//! No data is ever written to it, so `flush` skips stats and it costs almost
//! nothing.
//!
//! ```sh
//! cargo run --example 10_file_metadata
//! ```

use array_format::{ArrayFile, AttributeValue, FileConfig, NoCompression};

/// Reserved array name used to hold file-level metadata.
const FILE_META: &str = "__file__";

#[tokio::main]
async fn main() {
    let mut file = ArrayFile::create_memory(FileConfig::new(NoCompression))
        .await
        .unwrap();

    // A scalar placeholder: empty shape means no dimensions and no chunks, so
    // nothing is ever stored for it beyond its attributes.
    file.define_array::<u8>(FILE_META, vec![], vec![], None, None)
        .unwrap();

    file.set_attribute(FILE_META, "title", AttributeValue::String("My Dataset".into()))
        .unwrap();
    file.set_attribute(FILE_META, "schema_version", AttributeValue::Int32(3))
        .unwrap();
    file.set_attribute(
        FILE_META,
        "authors",
        AttributeValue::StringList(vec!["alice".into(), "bob".into()]),
    )
    .unwrap();

    // Persist — a metadata-only file is completely valid.
    file.flush().await.unwrap();

    // Read the file-level metadata back.
    let title = file.get_attribute(FILE_META, "title").unwrap();
    let version = file.get_attribute(FILE_META, "schema_version").unwrap();
    let authors = file.get_attribute(FILE_META, "authors").unwrap();
    println!("title          = {title:?}");
    println!("schema_version = {version:?}");
    println!("authors        = {authors:?}");

    // The placeholder shows up in list_arrays like any array; filter it out to
    // present only real data arrays to users.
    let data_arrays: Vec<String> = file
        .list_arrays()
        .into_iter()
        .map(|m| m.name)
        .filter(|name| name != FILE_META)
        .collect();
    println!("data arrays    = {data_arrays:?}"); // [] — metadata only

    assert_eq!(title, Some(&AttributeValue::String("My Dataset".into())));
    assert!(data_arrays.is_empty());
}
