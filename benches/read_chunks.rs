//! Benchmarks for reading chunked arrays from in-memory storage.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::Rng;
use tokio::runtime::Runtime;

use array_format::{
    DType, InMemoryStorage, Lz4Codec, NoCompression, Reader, Writer, WriterConfig, ZstdCodec,
};

const CHUNK_SIZE: usize = 64 * 1024; // 64 KiB per chunk
const NUM_CHUNKS: u32 = 64;
const BLOCK_TARGET: usize = 1024 * 1024; // 1 MiB blocks

/// Generates random chunk data that doesn't compress well (worst case).
fn random_chunk() -> Vec<u8> {
    let mut rng = rand::rng();
    (0..CHUNK_SIZE).map(|_| rng.random::<u8>()).collect()
}

/// Generates patterned chunk data that compresses well (best case).
fn patterned_chunk() -> Vec<u8> {
    (0..CHUNK_SIZE).map(|i| (i % 256) as u8).collect()
}

/// Write a chunked array using the given codec and return the storage.
async fn prepare_file<C: array_format::CompressionCodec + Clone>(
    codec: C,
    chunk_data: &[u8],
) -> InMemoryStorage {
    let storage = InMemoryStorage::new();
    let config = WriterConfig {
        block_target_size: BLOCK_TARGET,
        codec,
    };
    let mut writer = Writer::new(storage.clone(), config);
    let mut chunk_writer = writer
        .begin_chunked_array(
            "data",
            DType::Float32,
            vec!["x".into(), "y".into()],
            vec![NUM_CHUNKS, 1],
            vec![1, 1],
            None,
        )
        .unwrap();
    for i in 0..NUM_CHUNKS {
        chunk_writer.write(vec![i, 0], chunk_data).unwrap();
    }
    writer.flush().await.unwrap();
    storage
}

fn bench_read_all_chunks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let total_bytes = (CHUNK_SIZE * NUM_CHUNKS as usize) as u64;

    let mut group = c.benchmark_group("read_all_chunks");
    group.throughput(Throughput::Bytes(total_bytes));

    // --- NoCompression, patterned data ---
    let patterned = patterned_chunk();
    let storage_no_pat = rt.block_on(prepare_file(NoCompression, &patterned));
    group.bench_function(BenchmarkId::new("none", "patterned"), |b| {
        b.to_async(&rt).iter(|| async {
            let reader = Reader::open(storage_no_pat.clone(), 64 * 1024 * 1024)
                .await
                .unwrap();
            for i in 0..NUM_CHUNKS {
                reader.read_chunk_raw("data", &[i, 0]).await.unwrap();
            }
        })
    });

    // --- Zstd, patterned data ---
    let storage_zstd_pat = rt.block_on(prepare_file(ZstdCodec::default(), &patterned));
    group.bench_function(BenchmarkId::new("zstd", "patterned"), |b| {
        b.to_async(&rt).iter(|| async {
            let reader = Reader::open(storage_zstd_pat.clone(), 64 * 1024 * 1024)
                .await
                .unwrap();
            for i in 0..NUM_CHUNKS {
                reader.read_chunk_raw("data", &[i, 0]).await.unwrap();
            }
        })
    });

    // --- Lz4, patterned data ---
    let storage_lz4_pat = rt.block_on(prepare_file(Lz4Codec, &patterned));
    group.bench_function(BenchmarkId::new("lz4", "patterned"), |b| {
        b.to_async(&rt).iter(|| async {
            let reader = Reader::open(storage_lz4_pat.clone(), 64 * 1024 * 1024)
                .await
                .unwrap();
            for i in 0..NUM_CHUNKS {
                reader.read_chunk_raw("data", &[i, 0]).await.unwrap();
            }
        })
    });

    // --- NoCompression, random data ---
    let random = random_chunk();
    let storage_no_rnd = rt.block_on(prepare_file(NoCompression, &random));
    group.bench_function(BenchmarkId::new("none", "random"), |b| {
        b.to_async(&rt).iter(|| async {
            let reader = Reader::open(storage_no_rnd.clone(), 64 * 1024 * 1024)
                .await
                .unwrap();
            for i in 0..NUM_CHUNKS {
                reader.read_chunk_raw("data", &[i, 0]).await.unwrap();
            }
        })
    });

    // --- Zstd, random data ---
    let storage_zstd_rnd = rt.block_on(prepare_file(ZstdCodec::default(), &random));
    group.bench_function(BenchmarkId::new("zstd", "random"), |b| {
        b.to_async(&rt).iter(|| async {
            let reader = Reader::open(storage_zstd_rnd.clone(), 64 * 1024 * 1024)
                .await
                .unwrap();
            for i in 0..NUM_CHUNKS {
                reader.read_chunk_raw("data", &[i, 0]).await.unwrap();
            }
        })
    });

    // --- Lz4, random data ---
    let storage_lz4_rnd = rt.block_on(prepare_file(Lz4Codec, &random));
    group.bench_function(BenchmarkId::new("lz4", "random"), |b| {
        b.to_async(&rt).iter(|| async {
            let reader = Reader::open(storage_lz4_rnd.clone(), 64 * 1024 * 1024)
                .await
                .unwrap();
            for i in 0..NUM_CHUNKS {
                reader.read_chunk_raw("data", &[i, 0]).await.unwrap();
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

    // Pre-open readers so we benchmark only the read_chunk call (cache warm).
    let storage_no = rt.block_on(prepare_file(NoCompression, &patterned));
    let reader_no = rt.block_on(async {
        let r = Reader::open(storage_no, 64 * 1024 * 1024).await.unwrap();
        // Warm the cache.
        r.read_chunk_raw("data", &[0, 0]).await.unwrap();
        r
    });

    group.bench_function("none/cached", |b| {
        b.to_async(&rt).iter(|| async {
            reader_no.read_chunk_raw("data", &[0, 0]).await.unwrap();
        })
    });

    let storage_zstd = rt.block_on(prepare_file(ZstdCodec::default(), &patterned));
    let reader_zstd = rt.block_on(async {
        let r = Reader::open(storage_zstd, 64 * 1024 * 1024).await.unwrap();
        r.read_chunk_raw("data", &[0, 0]).await.unwrap();
        r
    });

    group.bench_function("zstd/cached", |b| {
        b.to_async(&rt).iter(|| async {
            reader_zstd.read_chunk_raw("data", &[0, 0]).await.unwrap();
        })
    });

    let storage_lz4 = rt.block_on(prepare_file(Lz4Codec, &patterned));
    let reader_lz4 = rt.block_on(async {
        let r = Reader::open(storage_lz4, 64 * 1024 * 1024).await.unwrap();
        r.read_chunk_raw("data", &[0, 0]).await.unwrap();
        r
    });

    group.bench_function("lz4/cached", |b| {
        b.to_async(&rt).iter(|| async {
            reader_lz4.read_chunk_raw("data", &[0, 0]).await.unwrap();
        })
    });

    group.finish();
}

criterion_group!(benches, bench_read_all_chunks, bench_single_chunk_read);
criterion_main!(benches);
