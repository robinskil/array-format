//! End-to-end roundtrip tests: write → read → delete → compact → re-read.

use array_format::{
    InMemoryStorage, NoCompression, PrimitiveArray, Reader, Writer, WriterConfig, compact,
};

fn small_config() -> WriterConfig<NoCompression> {
    WriterConfig {
        block_target_size: 64,
        codec: NoCompression,
    }
}

#[tokio::test]
async fn flat_array_roundtrip() {
    let storage = InMemoryStorage::new();

    // Write two flat arrays with different dtypes.
    {
        let mut w = Writer::new(storage.clone(), small_config());
        let ints = PrimitiveArray::<u8>::from_slice(&[1u8; 80]);
        w.write_array("ints", vec!["x".into()], &ints).unwrap();
        let floats = PrimitiveArray::<f64>::from_slice(&[0.0f64; 5]);
        w.write_array("floats", vec!["t".into()], &floats).unwrap();
        w.flush().await.unwrap();
    }

    // Read them back.
    let reader = Reader::open(storage, 4096).await.unwrap();
    assert_eq!(reader.list_arrays().len(), 2);
    let ints = reader.read_array::<u8>("ints").await.unwrap();
    assert_eq!(ints.values(), &[1u8; 80]);
    let floats = reader.read_array::<f64>("floats").await.unwrap();
    assert_eq!(floats.values(), &[0.0f64; 5]);
}

#[tokio::test]
async fn delete_and_compact() {
    let storage = InMemoryStorage::new();

    // Write 3 arrays.
    {
        let mut w = Writer::new(storage.clone(), small_config());
        let a = PrimitiveArray::<u8>::from_slice(&[10; 20]);
        w.write_array("a", vec![], &a).unwrap();
        let b = PrimitiveArray::<u16>::from_slice(&[20; 10]);
        w.write_array("b", vec![], &b).unwrap();
        let c = PrimitiveArray::<i64>::from_slice(&[30i64; 2]);
        w.write_array("c", vec![], &c).unwrap();
        w.delete("b").unwrap();
        w.flush().await.unwrap();
    }

    // Before compact: b is deleted but still in footer.
    {
        let r = Reader::open(storage.clone(), 1024).await.unwrap();
        assert_eq!(r.list_arrays().len(), 2);
        assert_eq!(r.footer().arrays.len(), 3); // all 3 in raw footer
    }

    // Compact.
    compact(&storage, &NoCompression, Some(64)).await.unwrap();

    // After compact: only a and c remain in footer.
    let r = Reader::open(storage, 1024).await.unwrap();
    assert_eq!(r.footer().arrays.len(), 2);
    let names: Vec<_> = r.list_arrays().iter().map(|a| a.name.clone()).collect();
    assert_eq!(names, vec!["a", "c"]);
    let a = r.read_array::<u8>("a").await.unwrap();
    assert_eq!(a.values(), &[10u8; 20]);
    let c = r.read_array::<i64>("c").await.unwrap();
    assert_eq!(c.values(), &[30i64; 2]);
}
