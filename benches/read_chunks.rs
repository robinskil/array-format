//! Benchmarks for reading chunked arrays from in-memory storage.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::Rng;
use tokio::runtime::Runtime;

use array_format::{ArrayFile, FileConfig, InMemoryStorage, Lz4Codec, NoCompression, ZstdCodec};

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

async fn prepare_file<C: array_format::CompressionCodec + Clone + 'static>(
    codec: C,
    chunk_data: &[u8],
) -> (ArrayFile, InMemoryStorage) {
    let config = FileConfig {
        block_target_size: BLOCK_TARGET,
        ..FileConfig::new(codec)
    };
    let mut file = ArrayFile::create_memory(config).await.unwrap();
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
    let overlay = InMemoryStorage::new();
    file.flush_memory(&overlay).await.unwrap();
    (file, overlay)
}

fn bench_read_all_chunks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let total_bytes = (CHUNK_SIZE * NUM_CHUNKS as usize) as u64;

    let mut group = c.benchmark_group("read_all_chunks");
    group.throughput(Throughput::Bytes(total_bytes));

    let patterned = patterned_chunk();
    let random = random_chunk();

    let (file_no_pat, _ov) = rt.block_on(prepare_file(NoCompression, &patterned));
    group.bench_function(BenchmarkId::new("none", "patterned"), |b| {
        b.to_async(&rt).iter(|| async {
            for i in 0..NUM_CHUNKS as usize {
                file_no_pat
                    .read_array::<u8>("data", vec![i, 0], vec![1, CHUNK_SIZE])
                    .await
                    .unwrap();
            }
        })
    });

    let (file_zstd_pat, _ov) = rt.block_on(prepare_file(ZstdCodec::default(), &patterned));
    group.bench_function(BenchmarkId::new("zstd", "patterned"), |b| {
        b.to_async(&rt).iter(|| async {
            for i in 0..NUM_CHUNKS as usize {
                file_zstd_pat
                    .read_array::<u8>("data", vec![i, 0], vec![1, CHUNK_SIZE])
                    .await
                    .unwrap();
            }
        })
    });

    let (file_lz4_pat, _ov) = rt.block_on(prepare_file(Lz4Codec, &patterned));
    group.bench_function(BenchmarkId::new("lz4", "patterned"), |b| {
        b.to_async(&rt).iter(|| async {
            for i in 0..NUM_CHUNKS as usize {
                file_lz4_pat
                    .read_array::<u8>("data", vec![i, 0], vec![1, CHUNK_SIZE])
                    .await
                    .unwrap();
            }
        })
    });

    let (file_no_rnd, _ov) = rt.block_on(prepare_file(NoCompression, &random));
    group.bench_function(BenchmarkId::new("none", "random"), |b| {
        b.to_async(&rt).iter(|| async {
            for i in 0..NUM_CHUNKS as usize {
                file_no_rnd
                    .read_array::<u8>("data", vec![i, 0], vec![1, CHUNK_SIZE])
                    .await
                    .unwrap();
            }
        })
    });

    let (file_zstd_rnd, _ov) = rt.block_on(prepare_file(ZstdCodec::default(), &random));
    group.bench_function(BenchmarkId::new("zstd", "random"), |b| {
        b.to_async(&rt).iter(|| async {
            for i in 0..NUM_CHUNKS as usize {
                file_zstd_rnd
                    .read_array::<u8>("data", vec![i, 0], vec![1, CHUNK_SIZE])
                    .await
                    .unwrap();
            }
        })
    });

    let (file_lz4_rnd, _ov) = rt.block_on(prepare_file(Lz4Codec, &random));
    group.bench_function(BenchmarkId::new("lz4", "random"), |b| {
        b.to_async(&rt).iter(|| async {
            for i in 0..NUM_CHUNKS as usize {
                file_lz4_rnd
                    .read_array::<u8>("data", vec![i, 0], vec![1, CHUNK_SIZE])
                    .await
                    .unwrap();
            }
        })
    });

    group.finish();
}

fn bench_single_chunk_read(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("single_chunk_read");
    group.throughput(Throughput::Bytes(CHUNK_SIZE as u64));

    let patterned = patterned_chunk();

    let (file_no, _ov) = rt.block_on(prepare_file(NoCompression, &patterned));
    rt.block_on(async {
        file_no
            .read_array::<u8>("data", vec![0, 0], vec![1, CHUNK_SIZE])
            .await
            .unwrap()
    });
    group.bench_function("none/cached", |b| {
        b.to_async(&rt).iter(|| async {
            file_no
                .read_array::<u8>("data", vec![0, 0], vec![1, CHUNK_SIZE])
                .await
                .unwrap();
        })
    });

    let (file_zstd, _ov) = rt.block_on(prepare_file(ZstdCodec::default(), &patterned));
    rt.block_on(async {
        file_zstd
            .read_array::<u8>("data", vec![0, 0], vec![1, CHUNK_SIZE])
            .await
            .unwrap()
    });
    group.bench_function("zstd/cached", |b| {
        b.to_async(&rt).iter(|| async {
            file_zstd
                .read_array::<u8>("data", vec![0, 0], vec![1, CHUNK_SIZE])
                .await
                .unwrap();
        })
    });

    let (file_lz4, _ov) = rt.block_on(prepare_file(Lz4Codec, &patterned));
    rt.block_on(async {
        file_lz4
            .read_array::<u8>("data", vec![0, 0], vec![1, CHUNK_SIZE])
            .await
            .unwrap()
    });
    group.bench_function("lz4/cached", |b| {
        b.to_async(&rt).iter(|| async {
            file_lz4
                .read_array::<u8>("data", vec![0, 0], vec![1, CHUNK_SIZE])
                .await
                .unwrap();
        })
    });

    group.finish();
}

criterion_group!(benches, bench_read_all_chunks, bench_single_chunk_read);
criterion_main!(benches);
