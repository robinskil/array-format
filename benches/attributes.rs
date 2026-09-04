//! Benchmarks for opening a file with many arrays and querying attributes.
//!
//! 100K arrays with 20 attributes each: `open()` builds the per-array
//! attribute maps, `attribute_index` reads one attribute across every array.

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use array_format::{
    ArrayFile, ArrayWriter, AttributeValue, NoCompression, ReadConfig, WriterConfig,
};
use object_store::{ObjectStore, local::LocalFileSystem};

const ARRAYS: usize = 100_000;
const KEYS: usize = 20;

async fn write_file(store: Arc<dyn ObjectStore>, path: object_store::path::Path) {
    let mut writer = ArrayWriter::new(WriterConfig::new(NoCompression));
    for i in 0..ARRAYS {
        let name = format!("array_{i:06}");
        writer
            .define_array::<f64>(&name, vec!["t".into()], vec![4], None, None)
            .unwrap();
        for k in 0..KEYS {
            // Values repeat every 50 arrays, as real attribute values do.
            let value = AttributeValue::Int64(((i % 50) * k) as i64);
            writer
                .set_attribute(&name, &format!("key_{k:02}"), value)
                .unwrap();
        }
    }
    writer.finish(store, path).await.unwrap();
}

fn bench_attributes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()) as Arc<dyn ObjectStore>;
    let path = object_store::path::Path::from("attrs.af");
    rt.block_on(write_file(Arc::clone(&store), path.clone()));

    let mut group = c.benchmark_group("attributes_100k_x_20");
    group.sample_size(10);
    group.bench_function("open", |b| {
        let store = Arc::clone(&store);
        let path = path.clone();
        b.to_async(&rt).iter(move || {
            let store = Arc::clone(&store);
            let path = path.clone();
            async move {
                ArrayFile::open(store, path, ReadConfig::default())
                    .await
                    .unwrap()
            }
        })
    });

    let file = rt
        .block_on(ArrayFile::open(store, path, ReadConfig::default()))
        .unwrap();
    group.bench_function("attribute_index/present_key", |b| {
        b.iter(|| file.attribute_index("key_09"))
    });
    group.bench_function("attribute_index/absent_key", |b| {
        b.iter(|| file.attribute_index("no_such_key"))
    });
    group.bench_function("get_attribute", |b| {
        b.iter(|| file.get_attribute("array_050000", "key_09"))
    });
    group.bench_function("attributes_map", |b| {
        b.iter(|| file.attributes("array_050000").map(|m| m.len()))
    });
    group.finish();
}

criterion_group!(benches, bench_attributes);
criterion_main!(benches);
