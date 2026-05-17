//! Benchmarks for reading chunked arrays from real files on disk.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::Rng;
use tokio::runtime::Runtime;

use array_format::{File, FileConfig, Lz4Codec, NoCompression, ZstdCodec};

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
    dir: &std::path::Path,
    filename: &str,
    codec: C,
    chunk_data: &[u8],
) -> std::path::PathBuf {
    let path = dir.join(filename);
    let config = FileConfig {
        block_target_size: BLOCK_TARGET,
        ..FileConfig::new(codec)
    };
    let mut file = File::create(&path, config).await.unwrap();
    file.define_array::<u8>(
        "data",
        vec!["x".into(), "y".into()],
        vec![NUM_CHUNKS as usize, CHUNK_SIZE],
        Some(vec![1, CHUNK_SIZE]),
        None,
    )
    .unwrap();
    for i in 0..NUM_CHUNKS as usize {
        let chunk = ndarray::Array::from_vec(chunk_data.to_vec()).into_dyn();
        file.write_array("data", vec![i, 0], chunk.view())
            .await
            .unwrap();
    }
    file.flush().await.unwrap();
    file.compact().await.unwrap();
    path
}

fn bench_file_read_all_chunks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let total_bytes = (CHUNK_SIZE * NUM_CHUNKS as usize) as u64;
    let tmp_dir = tempfile::tempdir().unwrap();

    let mut group = c.benchmark_group("file_read_all_chunks");
    group.throughput(Throughput::Bytes(total_bytes));

    let patterned = patterned_chunk();
    let random = random_chunk();

    macro_rules! bench_codec {
        ($codec:expr, $name:literal, $data:expr, $filename:literal) => {{
            let path = rt.block_on(prepare_file_on_disk(
                tmp_dir.path(),
                $filename,
                $codec,
                $data,
            ));
            group.bench_function(BenchmarkId::new($name, "patterned"), |b| {
                b.to_async(&rt).iter(|| async {
                    let cfg = FileConfig::new(NoCompression);
                    let file = File::open(&path, cfg).await.unwrap();
                    for i in 0..NUM_CHUNKS as usize {
                        file.read_array::<u8>("data", vec![i, 0], vec![1, CHUNK_SIZE])
                            .await
                            .unwrap();
                    }
                })
            });
        }};
    }

    bench_codec!(NoCompression, "none", &patterned, "none_pat.af");
    bench_codec!(ZstdCodec::default(), "zstd", &patterned, "zstd_pat.af");
    bench_codec!(Lz4Codec, "lz4", &patterned, "lz4_pat.af");

    let path_no_rnd = rt.block_on(prepare_file_on_disk(
        tmp_dir.path(),
        "none_rnd.af",
        NoCompression,
        &random,
    ));
    group.bench_function(BenchmarkId::new("none", "random"), |b| {
        b.to_async(&rt).iter(|| async {
            let file = File::open(&path_no_rnd, FileConfig::new(NoCompression))
                .await
                .unwrap();
            for i in 0..NUM_CHUNKS as usize {
                file.read_array::<u8>("data", vec![i, 0], vec![1, CHUNK_SIZE])
                    .await
                    .unwrap();
            }
        })
    });

    let path_zstd_rnd = rt.block_on(prepare_file_on_disk(
        tmp_dir.path(),
        "zstd_rnd.af",
        ZstdCodec::default(),
        &random,
    ));
    group.bench_function(BenchmarkId::new("zstd", "random"), |b| {
        b.to_async(&rt).iter(|| async {
            let file = File::open(&path_zstd_rnd, FileConfig::new(NoCompression))
                .await
                .unwrap();
            for i in 0..NUM_CHUNKS as usize {
                file.read_array::<u8>("data", vec![i, 0], vec![1, CHUNK_SIZE])
                    .await
                    .unwrap();
            }
        })
    });

    let path_lz4_rnd = rt.block_on(prepare_file_on_disk(
        tmp_dir.path(),
        "lz4_rnd.af",
        Lz4Codec,
        &random,
    ));
    group.bench_function(BenchmarkId::new("lz4", "random"), |b| {
        b.to_async(&rt).iter(|| async {
            let file = File::open(&path_lz4_rnd, FileConfig::new(NoCompression))
                .await
                .unwrap();
            for i in 0..NUM_CHUNKS as usize {
                file.read_array::<u8>("data", vec![i, 0], vec![1, CHUNK_SIZE])
                    .await
                    .unwrap();
            }
        })
    });

    group.finish();
}

fn bench_file_single_chunk_read(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let tmp_dir = tempfile::tempdir().unwrap();

    let mut group = c.benchmark_group("file_single_chunk_read");
    group.throughput(Throughput::Bytes(CHUNK_SIZE as u64));

    let patterned = patterned_chunk();

    let path_no = rt.block_on(prepare_file_on_disk(
        tmp_dir.path(),
        "single_none.af",
        NoCompression,
        &patterned,
    ));
    group.bench_function("none/uncached", |b| {
        b.to_async(&rt).iter(|| async {
            let cfg = FileConfig {
                cache_capacity: 0,
                ..FileConfig::new(NoCompression)
            };
            let file = File::open(&path_no, cfg).await.unwrap();
            file.read_array::<u8>("data", vec![0, 0], vec![1, CHUNK_SIZE])
                .await
                .unwrap();
        })
    });

    let path_zstd = rt.block_on(prepare_file_on_disk(
        tmp_dir.path(),
        "single_zstd.af",
        ZstdCodec::default(),
        &patterned,
    ));
    group.bench_function("zstd/uncached", |b| {
        b.to_async(&rt).iter(|| async {
            let cfg = FileConfig {
                cache_capacity: 0,
                ..FileConfig::new(NoCompression)
            };
            let file = File::open(&path_zstd, cfg).await.unwrap();
            file.read_array::<u8>("data", vec![0, 0], vec![1, CHUNK_SIZE])
                .await
                .unwrap();
        })
    });

    let path_lz4 = rt.block_on(prepare_file_on_disk(
        tmp_dir.path(),
        "single_lz4.af",
        Lz4Codec,
        &patterned,
    ));
    group.bench_function("lz4/uncached", |b| {
        b.to_async(&rt).iter(|| async {
            let cfg = FileConfig {
                cache_capacity: 0,
                ..FileConfig::new(NoCompression)
            };
            let file = File::open(&path_lz4, cfg).await.unwrap();
            file.read_array::<u8>("data", vec![0, 0], vec![1, CHUNK_SIZE])
                .await
                .unwrap();
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
