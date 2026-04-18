//! Benchmarks for parallel and concurrent reads of thousands of arrays.
//!
//! Each file contains 2,000 flat f32 arrays with 10K elements (40 KB) each.
//! The parallel strategy spawns `par` tokio tasks that each read their
//! partition of arrays concurrently via `buffer_unordered(conc)`.
//! Sequential reads serve as the baseline.

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use crossbeam_queue::SegQueue;
use futures::stream::{self, StreamExt};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use rand::Rng;
use tokio::runtime::Runtime;

use array_format::{
    InMemoryStorage, Lz4Codec, NoCompression, ObjectStoreBackend, PrimitiveArray, Reader, Writer,
    WriterConfig,
};

const MANY_ARRAYS_COUNT: usize = 25_000;
const ELEMENTS_PER_ARRAY: usize = 10_000; // 10 K f32 elements = 40 KB each
const BLOCK_TARGET: usize = 4 * 1024 * 1024; // 4 MiB blocks
const CACHE_SIZE: u64 = 256 * 1024 * 1024; // 256 MiB

fn total_bytes() -> u64 {
    (MANY_ARRAYS_COUNT * ELEMENTS_PER_ARRAY * std::mem::size_of::<f32>()) as u64
}

async fn prepare_many_arrays_on_disk<C: array_format::CompressionCodec + Clone>(
    dir: &std::path::Path,
    filename: &str,
    codec: C,
) -> (ObjectStoreBackend, Vec<String>) {
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
    let path = Path::from(filename);
    let backend = ObjectStoreBackend::new(store, path);

    let config = WriterConfig {
        block_target_size: BLOCK_TARGET,
        codec,
    };
    let mut writer = Writer::new(backend, config);

    let mut rng = rand::rng();
    let mut names = Vec::with_capacity(MANY_ARRAYS_COUNT);
    for i in 0..MANY_ARRAYS_COUNT {
        let name = format!("arr_{i:05}");
        let values: Vec<f32> = (0..ELEMENTS_PER_ARRAY)
            .map(|_| rng.random::<f32>())
            .collect();
        let array = PrimitiveArray::from_slice(&values);
        writer.write_array(&name, vec!["x".into()], &array).unwrap();
        names.push(name);
    }
    writer.flush().await.unwrap();

    let store = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
    let path = Path::from(filename);
    (ObjectStoreBackend::new(store, path), names)
}

async fn prepare_many_arrays_in_memory<C: array_format::CompressionCodec + Clone>(
    codec: C,
) -> (InMemoryStorage, Vec<String>) {
    let storage = InMemoryStorage::new();
    let config = WriterConfig {
        block_target_size: BLOCK_TARGET,
        codec,
    };
    let mut writer = Writer::new(storage.clone(), config);

    let mut rng = rand::rng();
    let mut names = Vec::with_capacity(MANY_ARRAYS_COUNT);
    for i in 0..MANY_ARRAYS_COUNT {
        let name = format!("arr_{i:05}");
        let values: Vec<f32> = (0..ELEMENTS_PER_ARRAY)
            .map(|_| rng.random::<f32>())
            .collect();
        let array = PrimitiveArray::from_slice(&values);
        writer.write_array(&name, vec!["x".into()], &array).unwrap();
        names.push(name);
    }
    writer.flush().await.unwrap();
    (storage, names)
}

// ── Helpers ─────────────────────────────────────────────────────────

async fn read_sequential(reader: &Reader, names: &[String]) {
    for name in names {
        reader.read_array::<f32>(name).await.unwrap();
    }
}

async fn read_parallel_concurrent(
    reader: Arc<Reader>,
    names: Vec<String>,
    parallelism: usize,
    concurrency: usize,
) {
    let queue = Arc::new(SegQueue::new());
    for name in names {
        queue.push(name);
    }

    let mut handles = Vec::with_capacity(parallelism);
    for _ in 0..parallelism {
        let reader = Arc::clone(&reader);
        let queue = Arc::clone(&queue);
        handles.push(tokio::spawn(async move {
            stream::iter(std::iter::from_fn({
                let queue = Arc::clone(&queue);
                move || queue.pop()
            }))
            .map(|name| {
                let reader = Arc::clone(&reader);
                async move {
                    reader.read_array::<f32>(&name).await.unwrap();
                }
            })
            .buffer_unordered(concurrency)
            .for_each(|_| async {})
            .await;
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

// ── In-memory benchmarks ────────────────────────────────────────────

fn bench_many_arrays_memory(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("many_arrays_memory");
    group.throughput(Throughput::Bytes(total_bytes()));
    group.sample_size(10);

    // ---- Lz4 ----
    let (storage_lz4, names_lz4) = rt.block_on(prepare_many_arrays_in_memory(Lz4Codec));

    group.bench_function(BenchmarkId::new("lz4", "sequential"), |b| {
        let names = &names_lz4;
        b.to_async(&rt).iter(|| {
            let storage = storage_lz4.clone();
            let names = names.clone();
            async move {
                let reader = Reader::open(storage, CACHE_SIZE).await.unwrap();
                read_sequential(&reader, &names).await;
            }
        })
    });

    {
        let (par, conc) = (24, 8);
        group.bench_function(
            BenchmarkId::new("lz4", format!("par{par}_conc{conc}")),
            |b| {
                let names = &names_lz4;
                b.to_async(&rt).iter(|| {
                    let storage = storage_lz4.clone();
                    let names = names.clone();
                    async move {
                        let reader = Arc::new(Reader::open(storage, CACHE_SIZE).await.unwrap());
                        read_parallel_concurrent(reader, names, par, conc).await;
                    }
                })
            },
        );
    }

    // ---- NoCompression ----
    let (storage_none, names_none) = rt.block_on(prepare_many_arrays_in_memory(NoCompression));

    group.bench_function(BenchmarkId::new("none", "sequential"), |b| {
        let names = &names_none;
        b.to_async(&rt).iter(|| {
            let storage = storage_none.clone();
            let names = names.clone();
            async move {
                let reader = Reader::open(storage, CACHE_SIZE).await.unwrap();
                read_sequential(&reader, &names).await;
            }
        })
    });

    {
        let (par, conc) = (24, 8);
        group.bench_function(
            BenchmarkId::new("none", format!("par{par}_conc{conc}")),
            |b| {
                let names = &names_none;
                b.to_async(&rt).iter(|| {
                    let storage = storage_none.clone();
                    let names = names.clone();
                    async move {
                        let reader = Arc::new(Reader::open(storage, CACHE_SIZE).await.unwrap());
                        read_parallel_concurrent(reader, names, par, conc).await;
                    }
                })
            },
        );
    }

    group.finish();
}

// ── File-backed benchmarks ──────────────────────────────────────────

fn bench_many_arrays_file(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let tmp_dir = tempfile::tempdir().unwrap();

    let mut group = c.benchmark_group("many_arrays_file");
    group.throughput(Throughput::Bytes(total_bytes()));
    group.sample_size(10);

    // ---- Lz4 on disk ----
    let (storage_lz4, names_lz4) = rt.block_on(prepare_many_arrays_on_disk(
        tmp_dir.path(),
        "many_lz4.af",
        Lz4Codec,
    ));

    group.bench_function(BenchmarkId::new("lz4", "sequential"), |b| {
        let names = &names_lz4;
        b.to_async(&rt).iter(|| {
            let storage = storage_lz4.clone();
            let names = names.clone();
            async move {
                let reader = Reader::open(storage, CACHE_SIZE).await.unwrap();
                read_sequential(&reader, &names).await;
            }
        })
    });

    {
        let (par, conc) = (24, 8);
        group.bench_function(
            BenchmarkId::new("lz4", format!("par{par}_conc{conc}")),
            |b| {
                let names = &names_lz4;
                b.to_async(&rt).iter(|| {
                    let storage = storage_lz4.clone();
                    let names = names.clone();
                    async move {
                        let reader = Arc::new(Reader::open(storage, CACHE_SIZE).await.unwrap());
                        read_parallel_concurrent(reader, names, par, conc).await;
                    }
                })
            },
        );
    }

    // ---- NoCompression on disk ----
    let (storage_none, names_none) = rt.block_on(prepare_many_arrays_on_disk(
        tmp_dir.path(),
        "many_none.af",
        NoCompression,
    ));

    group.bench_function(BenchmarkId::new("none", "sequential"), |b| {
        let names = &names_none;
        b.to_async(&rt).iter(|| {
            let storage = storage_none.clone();
            let names = names.clone();
            async move {
                let reader = Reader::open(storage, CACHE_SIZE).await.unwrap();
                read_sequential(&reader, &names).await;
            }
        })
    });

    {
        let (par, conc) = (24, 8);
        group.bench_function(
            BenchmarkId::new("none", format!("par{par}_conc{conc}")),
            |b| {
                let names = &names_none;
                b.to_async(&rt).iter(|| {
                    let storage = storage_none.clone();
                    let names = names.clone();
                    async move {
                        let reader = Arc::new(Reader::open(storage, CACHE_SIZE).await.unwrap());
                        read_parallel_concurrent(reader, names, par, conc).await;
                    }
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_many_arrays_memory, bench_many_arrays_file,);
criterion_main!(benches);
