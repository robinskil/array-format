//! Benchmarks for reading chunked arrays from real files on disk.

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::Rng;
use tokio::runtime::Runtime;

use array_format::{ArrayFile, FileConfig, Lz4Codec, NoCompression, ZstdCodec};
use object_store::{ObjectStore, local::LocalFileSystem};

const CHUNK_SIZE: usize = 64 * 1024; // 64 KiB per chunk
const NUM_CHUNKS: u32 = 64;
const BLOCK_TARGET: usize = 1024 * 1024; // 1 MiB blocks

fn random_chunk() -> Vec<u8> {
    let mut rng = rand::rng();
    (0..CHUNK_SIZE).map(|_| rng.random::<u8>()).collect()
}

fn patterned_chunk() -> Vec<u8> {
    (0..CHUNK_SIZE).map(|i| (i % 256) as u8).collect()
}

async fn prepare_file_on_disk<C: array_format::CompressionCodec + Clone + 'static>(
    store: Arc<dyn ObjectStore>,
    path: object_store::path::Path,
    codec: C,
    chunk_data: &[u8],
) {
    let config = FileConfig {
        block_target_size: BLOCK_TARGET,
        ..FileConfig::new(codec)
    };
    let mut file = ArrayFile::create(Arc::clone(&store), path, config).await.unwrap();
    file.define_array::<u8>(
        "data",
        vec!["x".into(), "y".into()],
        vec![NUM_CHUNKS as usize, CHUNK_SIZE],
        Some(vec![1, CHUNK_SIZE]),
        None,
    )
    .unwrap();
    for i in 0..NUM_CHUNKS as usize {
        let chunk = ndarray::Array::from_shape_vec(ndarray::IxDyn(&[1, CHUNK_SIZE]), chunk_data.to_vec()).unwrap();
        file.write_array("data", vec![i, 0], chunk.view())
            .await
            .unwrap();
    }
    file.flush().await.unwrap();
    file.compact().await.unwrap();
}

fn bench_file_read_all_chunks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let total_bytes = (CHUNK_SIZE * NUM_CHUNKS as usize) as u64;
    let tmp_dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path()).unwrap())
        as Arc<dyn ObjectStore>;

    let mut group = c.benchmark_group("file_read_all_chunks");
    group.throughput(Throughput::Bytes(total_bytes));

    let patterned = patterned_chunk();
    let random = random_chunk();

    macro_rules! bench_codec {
        ($codec:expr, $name:literal, $data:expr, $filename:literal) => {{
            let path = object_store::path::Path::from($filename);
            rt.block_on(prepare_file_on_disk(Arc::clone(&store), path.clone(), $codec, $data));
            let s = Arc::clone(&store);
            group.bench_function(BenchmarkId::new($name, "patterned"), |b| {
                let store = Arc::clone(&s);
                let path = path.clone();
                b.to_async(&rt).iter(move || {
                    let store = Arc::clone(&store);
                    let path = path.clone();
                    async move {
                        let cfg = FileConfig::new(NoCompression);
                        let file = ArrayFile::open(store, path, cfg).await.unwrap();
                        for i in 0..NUM_CHUNKS as usize {
                            file.read_array::<u8>("data", vec![i, 0], vec![1, CHUNK_SIZE])
                                .await
                                .unwrap();
                        }
                    }
                })
            });
        }};
    }

    bench_codec!(NoCompression, "none", &patterned, "none_pat.af");
    bench_codec!(ZstdCodec::default(), "zstd", &patterned, "zstd_pat.af");
    bench_codec!(Lz4Codec, "lz4", &patterned, "lz4_pat.af");

    let path_no_rnd = object_store::path::Path::from("none_rnd.af");
    rt.block_on(prepare_file_on_disk(Arc::clone(&store), path_no_rnd.clone(), NoCompression, &random));
    group.bench_function(BenchmarkId::new("none", "random"), |b| {
        let store = Arc::clone(&store);
        let path = path_no_rnd.clone();
        b.to_async(&rt).iter(move || {
            let store = Arc::clone(&store);
            let path = path.clone();
            async move {
                let file = ArrayFile::open(store, path, FileConfig::new(NoCompression)).await.unwrap();
                for i in 0..NUM_CHUNKS as usize {
                    file.read_array::<u8>("data", vec![i, 0], vec![1, CHUNK_SIZE])
                        .await
                        .unwrap();
                }
            }
        })
    });

    let path_zstd_rnd = object_store::path::Path::from("zstd_rnd.af");
    rt.block_on(prepare_file_on_disk(Arc::clone(&store), path_zstd_rnd.clone(), ZstdCodec::default(), &random));
    group.bench_function(BenchmarkId::new("zstd", "random"), |b| {
        let store = Arc::clone(&store);
        let path = path_zstd_rnd.clone();
        b.to_async(&rt).iter(move || {
            let store = Arc::clone(&store);
            let path = path.clone();
            async move {
                let file = ArrayFile::open(store, path, FileConfig::new(NoCompression)).await.unwrap();
                for i in 0..NUM_CHUNKS as usize {
                    file.read_array::<u8>("data", vec![i, 0], vec![1, CHUNK_SIZE])
                        .await
                        .unwrap();
                }
            }
        })
    });

    let path_lz4_rnd = object_store::path::Path::from("lz4_rnd.af");
    rt.block_on(prepare_file_on_disk(Arc::clone(&store), path_lz4_rnd.clone(), Lz4Codec, &random));
    group.bench_function(BenchmarkId::new("lz4", "random"), |b| {
        let store = Arc::clone(&store);
        let path = path_lz4_rnd.clone();
        b.to_async(&rt).iter(move || {
            let store = Arc::clone(&store);
            let path = path.clone();
            async move {
                let file = ArrayFile::open(store, path, FileConfig::new(NoCompression)).await.unwrap();
                for i in 0..NUM_CHUNKS as usize {
                    file.read_array::<u8>("data", vec![i, 0], vec![1, CHUNK_SIZE])
                        .await
                        .unwrap();
                }
            }
        })
    });

    group.finish();
}

fn bench_file_single_chunk_read(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let tmp_dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path()).unwrap())
        as Arc<dyn ObjectStore>;

    let mut group = c.benchmark_group("file_single_chunk_read");
    group.throughput(Throughput::Bytes(CHUNK_SIZE as u64));

    let patterned = patterned_chunk();

    let path_no = object_store::path::Path::from("single_none.af");
    rt.block_on(prepare_file_on_disk(Arc::clone(&store), path_no.clone(), NoCompression, &patterned));
    group.bench_function("none/uncached", |b| {
        let store = Arc::clone(&store);
        let path = path_no.clone();
        b.to_async(&rt).iter(move || {
            let store = Arc::clone(&store);
            let path = path.clone();
            async move {
                let cfg = FileConfig {
                    cache_capacity: 0,
                    ..FileConfig::new(NoCompression)
                };
                let file = ArrayFile::open(store, path, cfg).await.unwrap();
                file.read_array::<u8>("data", vec![0, 0], vec![1, CHUNK_SIZE])
                    .await
                    .unwrap();
            }
        })
    });

    let path_zstd = object_store::path::Path::from("single_zstd.af");
    rt.block_on(prepare_file_on_disk(Arc::clone(&store), path_zstd.clone(), ZstdCodec::default(), &patterned));
    group.bench_function("zstd/uncached", |b| {
        let store = Arc::clone(&store);
        let path = path_zstd.clone();
        b.to_async(&rt).iter(move || {
            let store = Arc::clone(&store);
            let path = path.clone();
            async move {
                let cfg = FileConfig {
                    cache_capacity: 0,
                    ..FileConfig::new(NoCompression)
                };
                let file = ArrayFile::open(store, path, cfg).await.unwrap();
                file.read_array::<u8>("data", vec![0, 0], vec![1, CHUNK_SIZE])
                    .await
                    .unwrap();
            }
        })
    });

    let path_lz4 = object_store::path::Path::from("single_lz4.af");
    rt.block_on(prepare_file_on_disk(Arc::clone(&store), path_lz4.clone(), Lz4Codec, &patterned));
    group.bench_function("lz4/uncached", |b| {
        let store = Arc::clone(&store);
        let path = path_lz4.clone();
        b.to_async(&rt).iter(move || {
            let store = Arc::clone(&store);
            let path = path.clone();
            async move {
                let cfg = FileConfig {
                    cache_capacity: 0,
                    ..FileConfig::new(NoCompression)
                };
                let file = ArrayFile::open(store, path, cfg).await.unwrap();
                file.read_array::<u8>("data", vec![0, 0], vec![1, CHUNK_SIZE])
                    .await
                    .unwrap();
            }
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_file_read_all_chunks,
    bench_file_single_chunk_read,
);
criterion_main!(benches);
