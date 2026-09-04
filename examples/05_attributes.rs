//! Per-array key-value attributes (units, metadata, …).
//!
//! ```sh
//! cargo run --example 05_attributes
//! ```

use std::sync::Arc;

use array_format::{ArrayWriter, AttributeValue, NoCompression, WriterConfig};
use object_store::memory::InMemory;

#[tokio::main]
async fn main() {
    let mut writer = ArrayWriter::new(WriterConfig::new(NoCompression));

    writer
        .define_array::<f32>("pressure", vec!["z".into()], vec![10], None, None)
        .unwrap();
    writer
        .set_attribute("pressure", "units", AttributeValue::String("hPa".into()))
        .unwrap();
    writer
        .set_attribute("pressure", "scale_factor", AttributeValue::Float64(0.01))
        .unwrap();
    writer
        .set_attribute("pressure", "valid_min", AttributeValue::Float32(0.0))
        .unwrap();
    // Attribute values can also be raw bytes or a list of values.
    writer
        .set_attribute(
            "pressure",
            "checksum",
            AttributeValue::Binary(Box::new([0xde, 0xad, 0xbe, 0xef])),
        )
        .unwrap();
    writer
        .set_attribute(
            "pressure",
            "valid_range",
            AttributeValue::Float32List(Box::new([0.0, 1100.0])),
        )
        .unwrap();

    // More arrays, with and without the same "units" attribute.
    writer
        .define_array::<f32>("temperature", vec!["z".into()], vec![10], None, None)
        .unwrap();
    writer
        .set_attribute("temperature", "units", AttributeValue::String("K".into()))
        .unwrap();
    writer
        .define_array::<f32>("humidity", vec!["z".into()], vec![10], None, None)
        .unwrap();
    // "humidity" deliberately has no "units" attribute.

    let file = writer
        .finish(
            Arc::new(InMemory::new()),
            object_store::path::Path::from("attrs.af"),
        )
        .await
        .unwrap();

    // Attributes are in memory from the moment the file opens: a lookup is
    // two hash probes, no I/O.
    let units = file.get_attribute("pressure", "units").unwrap();
    let scale = file.get_attribute("pressure", "scale_factor").unwrap();
    let missing = file.get_attribute("pressure", "long_name");
    let checksum = file.get_attribute("pressure", "checksum").unwrap();
    let range = file.get_attribute("pressure", "valid_range").unwrap();

    println!("units        = {units:?}");
    println!("scale_factor = {scale:?}");
    println!("long_name    = {missing:?}"); // None — not set
    println!("checksum     = {checksum:?}");
    println!("valid_range  = {range:?}");

    assert!(matches!(units, AttributeValue::String(s) if s == "hPa"));
    assert!(missing.is_none());
    assert!(matches!(checksum, AttributeValue::Binary(b) if b[..] == [0xde, 0xad, 0xbe, 0xef]));
    assert!(matches!(range, AttributeValue::Float32List(v) if v[..] == [0.0, 1100.0]));

    // All attributes of one array, as a map.
    println!(
        "\npressure has {} attributes",
        file.attributes("pressure").unwrap().count()
    );

    // One attribute across every array as a plain slice, aligned with arrays().
    let units = file.attribute_column("units").unwrap();
    assert_eq!(units.len(), file.arrays().len());

    // attribute_index gives the value of one attribute across every array in a
    // single call — a full column, with None where the attribute is absent.
    // Use it to prune (select arrays by attribute) without walking each one.
    println!("\nunits across all arrays:");
    let pressure_arrays: Vec<&str> = file
        .attribute_index("units")
        .into_iter()
        .inspect(|(name, value)| println!("  {name:<12} = {value:?}"))
        .filter(|(_, value)| matches!(value, Some(AttributeValue::String(s)) if s == "hPa"))
        .map(|(name, _)| name)
        .collect();

    println!("arrays measured in hPa: {pressure_arrays:?}");
    assert_eq!(pressure_arrays, vec!["pressure"]);
}
