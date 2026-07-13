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
    // Attribute values can also be raw bytes or a list of values.
    file.set_attribute(
        "pressure",
        "checksum",
        AttributeValue::Binary(vec![0xde, 0xad, 0xbe, 0xef]),
    )
    .unwrap();
    file.set_attribute(
        "pressure",
        "valid_range",
        AttributeValue::Float32List(vec![0.0, 1100.0]),
    )
    .unwrap();

    let units = file.get_attribute("pressure", "units").unwrap().unwrap();
    let scale = file
        .get_attribute("pressure", "scale_factor")
        .unwrap()
        .unwrap();
    let missing = file.get_attribute("pressure", "long_name").unwrap();
    let checksum = file.get_attribute("pressure", "checksum").unwrap().unwrap();
    let range = file.get_attribute("pressure", "valid_range").unwrap().unwrap();

    println!("units        = {units:?}");
    println!("scale_factor = {scale:?}");
    println!("long_name    = {missing:?}"); // None — not set
    println!("checksum     = {checksum:?}");
    println!("valid_range  = {range:?}");

    assert!(matches!(units, AttributeValue::String(s) if s == "hPa"));
    assert!(missing.is_none());
    assert!(matches!(checksum, AttributeValue::Binary(b) if b == &[0xde, 0xad, 0xbe, 0xef]));
    assert!(matches!(range, AttributeValue::Float32List(v) if v == &[0.0, 1100.0]));

    // Define more arrays with (and without) the same "units" attribute.
    file.define_array::<f32>("temperature", vec!["z".into()], vec![10], None, None)
        .unwrap();
    file.set_attribute("temperature", "units", AttributeValue::String("K".into()))
        .unwrap();
    file.define_array::<f32>("humidity", vec!["z".into()], vec![10], None, None)
        .unwrap();
    // "humidity" deliberately has no "units" attribute.

    // attribute_index gives the value of one attribute across every array in a
    // single call — a full column, with None where the attribute is absent.
    // Use it to prune (select arrays by attribute) without walking each one.
    println!("\nunits across all arrays:");
    let pressure_arrays: Vec<String> = file
        .attribute_index("units")
        .into_iter()
        .inspect(|(name, value)| println!("  {name:<12} = {value:?}"))
        .filter(|(_, value)| matches!(value, Some(AttributeValue::String(s)) if s == "hPa"))
        .map(|(name, _)| name)
        .collect();

    println!("arrays measured in hPa: {pressure_arrays:?}");
    assert_eq!(pressure_arrays, vec!["pressure".to_string()]);
}
