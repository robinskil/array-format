//! Benchmarks for reading chunked arrays from real files on disk.

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use rand::Rng;
use tokio::runtime::Runtime;

use array_format::{
    DType, Lz4Codec, NoCompression, ObjectStoreBackend, Reader, Writer, WriterConfig, ZstdCodec,
};

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

async fn prepare_file_on_disk<C: array_format::CompressionCodec + Clone>(
    dir: &std::path::Path,
    filename: &str,
    codec: C,
    chunk_data: &[u8],
) -> ObjectStoreBackend {
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
    let path = Path::from(filename);
    let backend = ObjectStoreBackend::new(store, path);

    let config = WriterConfig {
        block_target_size: BLOCK_TARGET,
        codec,
    };
    let mut writer = Writer::new(backend, config);
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

    let store = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
    let path = Path::from(filename);
    ObjectStoreBackend::new(store, path)
}

fn bench_file_read_all_chunks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let total_bytes = (CHUNK_SIZE * NUM_CHUNKS as usize) as u64;
    let tmp_dir = tempfile::tempdir().unwrap();

    let mut group = c.benchmark_group("file_read_all_chunks");
    group.throughput(Throughput::Bytes(total_bytes));

    let patterned = patterned_chunk();
    let random = random_chunk();

    let storage = rt.block_on(prepare_file_on_disk(
        tmp_dir.path(),
        "none_pat.af",
        NoCompression,
        &patterned,
    ));
    group.bench_function(BenchmarkId::new("none", "patterned"), |b| {
        b.to_async(&rt).iter(|| async {
            let reader = Reader::open(storage.clone(), 0).await.unwrap();
            for i in 0..NUM_CHUNKS {
                reader.read_chunk_raw("data", &[i, 0]).await.unwrap();
            }
        })
    });

    let storage = rt.block_on(prepare_file_on_disk(
        tmp_dir.path(),
        "zstd_pat.af",
        ZstdCodec::default(),
        &patterned,
    ));
    group.bench_function(BenchmarkId::new("zstd", "patterned"), |b| {
        b.to_async(&rt).iter(|| async {
            let reader = Reader::open(storage.clone(), 0).await.unwrap();
            for i in 0..NUM_CHUNKS {
                reader.read_chunk_raw("data", &[i, 0]).await.unwrap();
            }
        })
    });

    let storage = rt.block_on(prepare_file_on_disk(
        tmp_dir.path(),
        "lz4_pat.af",
        Lz4Codec,
        &patterned,
    ));
    group.bench_function(BenchmarkId::new("lz4", "patterned"), |b| {
        b.to_async(&rt).iter(|| async {
            let reader = Reader::open(storage.clone(), 0).await.unwrap();
            for i in 0..NUM_CHUNKS {
                reader.read_chunk_raw("data", &[i, 0]).await.unwrap();
            }
        })
    });

    let storage = rt.block_on(prepare_file_on_disk(
        tmp_dir.path(),
        "none_rnd.af",
        NoCompression,
        &random,
    ));
    group.bench_function(BenchmarkId::new("none", "random"), |b| {
        b.to_async(&rt).iter(|| async {
            let reader = Reader::open(storage.clone(), 0).await.unwrap();
            for i in 0..NUM_CHUNKS {
                reader.read_chunk_raw("data", &[i, 0]).await.unwrap();
            }
        })
    });

    let storage = rt.block_on(prepare_file_on_disk(
        tmp_dir.path(),
        "zstd_rnd.af",
        ZstdCodec::default(),
        &random,
    ));
    group.bench_function(BenchmarkId::new("zstd", "random"), |b| {
        b.to_async(&rt).iter(|| async {
            let reader = Reader::open(storage.clone(), 0).await.unwrap();
            for i in 0..NUM_CHUNKS {
                reader.read_chunk_raw("data", &[i, 0]).await.unwrap();
            }
        })
    });

    let storage = rt.block_on(prepare_file_on_disk(
        tmp_dir.path(),
        "lz4_rnd.af",
        Lz4Codec,
        &random,
    ));
    group.bench_function(BenchmarkId::new("lz4", "random"), |b| {
        b.to_async(&rt).iter(|| async {
            let reader = Reader::open(storage.clone(), 0).await.unwrap();
            for i in 0..NUM_CHUNKS {
                reader.read_chunk_raw("data", &[i, 0]).await.unwrap();
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

    let storage = rt.block_on(prepare_file_on_disk(
        tmp_dir.path(),
        "single_none.af",
        NoCompression,
        &patterned,
    ));
    let reader_no = rt.block_on(async { Reader::open(storage, 0).await.unwrap() });
    group.bench_function("none/uncached", |b| {
        b.to_async(&rt).iter(|| async {
            reader_no.read_chunk_raw("data", &[0, 0]).await.unwrap();
        })
    });

    let storage = rt.block_on(prepare_file_on_disk(
        tmp_dir.path(),
        "single_zstd.af",
        ZstdCodec::default(),
        &patterned,
    ));
    let reader_zstd = rt.block_on(async { Reader::open(storage, 0).await.unwrap() });
    group.bench_function("zstd/uncached", |b| {
        b.to_async(&rt).iter(|| async {
            reader_zstd.read_chunk_raw("data", &[0, 0]).await.unwrap();
        })
    });

    let storage = rt.block_on(prepare_file_on_disk(
        tmp_dir.path(),
        "single_lz4.af",
        Lz4Codec,
        &patterned,
    ));
    let reader_lz4 = rt.block_on(async { Reader::open(storage, 0).await.unwrap() });
    group.bench_function("lz4/uncached", |b| {
        b.to_async(&rt).iter(|| async {
            reader_lz4.read_chunk_raw("data", &[0, 0]).await.unwrap();
        })
    });

    let storage = rt.block_on(prepare_file_on_disk(
        tmp_dir.path(),
        "single_cached_none.af",
        NoCompression,
        &patterned,
    ));
    let reader_no_cached = rt.block_on(async {
        let r = Reader::open(storage, 64 * 1024 * 1024).await.unwrap();
        r.read_chunk_raw("data", &[0, 0]).await.unwrap();
        r
    });
    group.bench_function("none/cached", |b| {
        b.to_async(&rt).iter(|| async {
            reader_no_cached
                .read_chunk_raw("data", &[0, 0])
                .await
                .unwrap();
        })
    });

    let storage = rt.block_on(prepare_file_on_disk(
        tmp_dir.path(),
        "single_cached_zstd.af",
        ZstdCodec::default(),
        &patterned,
    ));
    let reader_zstd_cached = rt.block_on(async {
        let r = Reader::open(storage, 64 * 1024 * 1024).await.unwrap();
        r.read_chunk_raw("data", &[0, 0]).await.unwrap();
        r
    });
    group.bench_function("zstd/cached", |b| {
        b.to_async(&rt).iter(|| async {
            reader_zstd_cached
                .read_chunk_raw("data", &[0, 0])
                .await
                .unwrap();
        })
    });

    let storage = rt.block_on(prepare_file_on_disk(
        tmp_dir.path(),
        "single_cached_lz4.af",
        Lz4Codec,
        &patterned,
    ));
    let reader_lz4_cached = rt.block_on(async {
        let r = Reader::open(storage, 64 * 1024 * 1024).await.unwrap();
        r.read_chunk_raw("data", &[0, 0]).await.unwrap();
        r
    });
    group.bench_function("lz4/cached", |b| {
        b.to_async(&rt).iter(|| async {
            reader_lz4_cached
                .read_chunk_raw("data", &[0, 0])
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
