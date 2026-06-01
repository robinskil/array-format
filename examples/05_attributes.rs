//! Per-array key-value attributes (units, metadata, …).
//!
//! ```sh
//! cargo run --example 05_attributes
//! ```

use array_format::{ArrayFile, AttributeValue, FileConfig, NoCompression};

#[tokio::main]
async fn main() {
    let mut file = ArrayFile::create_memory(FileConfig::new(NoCompression))
        .await
        .unwrap();

    file.define_array::<f32>("pressure", vec!["z".into()], vec![10], None, None)
        .unwrap();
    file.set_attribute("pressure", "units", AttributeValue::String("hPa".into()))
        .unwrap();
    file.set_attribute("pressure", "scale_factor", AttributeValue::Float64(0.01))
        .unwrap();
    file.set_attribute("pressure", "valid_min", AttributeValue::Float32(0.0))
        .unwrap();

    let units = file.get_attribute("pressure", "units").unwrap().unwrap();
    let scale = file
        .get_attribute("pressure", "scale_factor")
        .unwrap()
        .unwrap();
    let missing = file.get_attribute("pressure", "long_name").unwrap();

    println!("units        = {units:?}");
    println!("scale_factor = {scale:?}");
    println!("long_name    = {missing:?}"); // None — not set

    assert!(matches!(units, AttributeValue::String(s) if s == "hPa"));
    assert!(missing.is_none());
}
