//! Benchmarks for parallel and concurrent reads of thousands of arrays.
//!
//! Each file contains 2,000 flat f32 arrays with 10K elements (40 KB) each.
//! The parallel strategy spawns `par` tokio tasks that each read their
//! partition of arrays concurrently via `buffer_unordered(conc)`.
//! Sequential reads serve as the baseline.

use std::collections::HashMap;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use crossbeam_queue::SegQueue;
use futures::stream::{self, StreamExt};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use rand::Rng;
use tokio::runtime::Runtime;

use array_format::{
    ArrayLayout, BlockId, InMemoryStorage, Lz4Codec, NoCompression, ObjectStoreBackend,
    PrimitiveArray, Reader, Writer, WriterConfig,
};

const MANY_ARRAYS_COUNT: usize = 25_000;
const ELEMENTS_PER_ARRAY: usize = 10_000; // 10 K i32 elements = 40 KB each
const BLOCK_TARGET: usize = 4 * 1024 * 1024; // 4 MiB blocks
const CACHE_SIZE: u64 = 256 * 1024 * 1024; // 256 MiB — ~25% of total data

fn total_bytes() -> u64 {
    (MANY_ARRAYS_COUNT * ELEMENTS_PER_ARRAY * std::mem::size_of::<i32>()) as u64
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
        let values: Vec<i32> = (0..ELEMENTS_PER_ARRAY).map(|_| 1).collect();
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
        let values: Vec<i32> = (0..ELEMENTS_PER_ARRAY).map(|_| 1).collect();
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
        reader.read_array::<i32>(name).await.unwrap();
    }
}

/// Groups array names by their block id so each task processes arrays
/// from distinct blocks, eliminating cache-load contention.
fn group_by_block(reader: &Reader) -> Vec<Vec<String>> {
    let mut block_map: HashMap<BlockId, Vec<String>> = HashMap::new();
    for meta in reader.list_arrays() {
        let block_id = match &meta.layout {
            ArrayLayout::Flat { address } => address.block_id,
            ArrayLayout::Chunked { chunks, .. } => {
                // Use the first chunk's block as representative.
                chunks.first().unwrap().1.block_id
            }
        };
        block_map
            .entry(block_id)
            .or_default()
            .push(meta.name.clone());
    }
    block_map.into_values().collect()
}

async fn read_parallel_concurrent(reader: Arc<Reader>, parallelism: usize, concurrency: usize) {
    let block_groups = group_by_block(&reader);

    // One queue per task; assign block groups round-robin across tasks.
    let queues: Vec<Arc<SegQueue<Vec<String>>>> = (0..parallelism)
        .map(|_| Arc::new(SegQueue::new()))
        .collect();
    for (i, group) in block_groups.into_iter().enumerate() {
        queues[i % parallelism].push(group);
    }
    let queues = Arc::new(queues);
    let time = std::time::Instant::now();
    let mut handles = Vec::with_capacity(parallelism);
    for task_id in 0..parallelism {
        let reader = Arc::clone(&reader);
        let queues = Arc::clone(&queues);
        handles.push(tokio::spawn(async move {
            loop {
                // Try own queue first, then steal from others.
                let own = &queues[task_id];
                let batch = own.pop().or_else(|| {
                    (1..queues.len())
                        .map(|off| (task_id + off) % queues.len())
                        .find_map(|v| queues[v].pop())
                });
                let Some(names) = batch else { break };

                // Read all arrays in this block-group concurrently.
                stream::iter(names)
                    .map(|name| {
                        let reader = Arc::clone(&reader);
                        async move {
                            reader.read_array::<i32>(&name).await.unwrap();
                        }
                    })
                    .buffered(concurrency)
                    .for_each(|_| async {})
                    .await;
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    println!(
        "parallel read with par={parallelism} conc={concurrency} took {:?}",
        time.elapsed()
    );
}

// ── In-memory benchmarks ────────────────────────────────────────────

fn bench_many_arrays_memory(c: &mut Criterion) {
    let rt_builder = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(32)
        .build()
        .unwrap();
    let rt = rt_builder;

    let mut group = c.benchmark_group("many_arrays_memory");
    group.throughput(Throughput::Bytes(total_bytes()));
    group.sample_size(10);

    // ---- Lz4 ----
    let (storage_lz4, names_lz4) = rt.block_on(prepare_many_arrays_in_memory(Lz4Codec));
    let reader_lz4 = rt
        .block_on(async { Arc::new(Reader::open(storage_lz4.clone(), CACHE_SIZE).await.unwrap()) });

    group.bench_function(BenchmarkId::new("lz4", "sequential"), |b| {
        let names = &names_lz4;
        let reader = Arc::clone(&reader_lz4);
        b.to_async(&rt).iter(|| {
            let reader = Arc::clone(&reader);
            let names = names.clone();
            async move {
                read_sequential(&reader, &names).await;
            }
        })
    });

    {
        let (par, conc) = (32, 32);
        group.bench_function(
            BenchmarkId::new("lz4", format!("par{par}_conc{conc}")),
            |b| {
                let reader = Arc::clone(&reader_lz4);
                b.to_async(&rt).iter(|| {
                    let reader = Arc::clone(&reader);
                    async move {
                        read_parallel_concurrent(reader, par, conc).await;
                    }
                })
            },
        );
    }

    // ---- NoCompression ----
    let (storage_none, names_none) = rt.block_on(prepare_many_arrays_in_memory(NoCompression));
    let reader_none = rt.block_on(async {
        Arc::new(
            Reader::open(storage_none.clone(), CACHE_SIZE)
                .await
                .unwrap(),
        )
    });

    group.bench_function(BenchmarkId::new("none", "sequential"), |b| {
        let names = &names_none;
        let reader = Arc::clone(&reader_none);
        b.to_async(&rt).iter(|| {
            let reader = Arc::clone(&reader);
            let names = names.clone();
            async move {
                read_sequential(&reader, &names).await;
            }
        })
    });

    {
        let (par, conc) = (32, 32);
        group.bench_function(
            BenchmarkId::new("none", format!("par{par}_conc{conc}")),
            |b| {
                let reader = Arc::clone(&reader_none);
                b.to_async(&rt).iter(|| {
                    let reader = Arc::clone(&reader);
                    async move {
                        read_parallel_concurrent(reader, par, conc).await;
                    }
                })
            },
        );
    }

    group.finish();
}

// ── File-backed benchmarks ──────────────────────────────────────────

fn bench_many_arrays_file(c: &mut Criterion) {
    let rt_builder = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(32)
        .build()
        .unwrap();
    let rt = rt_builder;
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
    let reader_lz4 = rt
        .block_on(async { Arc::new(Reader::open(storage_lz4.clone(), CACHE_SIZE).await.unwrap()) });

    {
        let (par, conc) = (32, 32);
        group.bench_function(
            BenchmarkId::new("lz4", format!("par{par}_conc{conc}")),
            |b| {
                let reader = Arc::clone(&reader_lz4);
                b.to_async(&rt).iter(|| {
                    let reader = Arc::clone(&reader);
                    async move {
                        read_parallel_concurrent(reader, par, conc).await;
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
    let reader_none = rt.block_on(async {
        Arc::new(
            Reader::open(storage_none.clone(), CACHE_SIZE)
                .await
                .unwrap(),
        )
    });

    group.bench_function(BenchmarkId::new("none", "sequential"), |b| {
        let names = &names_none;
        let reader = Arc::clone(&reader_none);
        b.to_async(&rt).iter(|| {
            let reader = Arc::clone(&reader);
            let names = names.clone();
            async move {
                read_sequential(&reader, &names).await;
            }
        })
    });

    {
        let (par, conc) = (32, 32);
        group.bench_function(
            BenchmarkId::new("none", format!("par{par}_conc{conc}")),
            |b| {
                let reader = Arc::clone(&reader_none);
                b.to_async(&rt).iter(|| {
                    let reader = Arc::clone(&reader);
                    async move {
                        read_parallel_concurrent(reader, par, conc).await;
                    }
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_many_arrays_file,);
criterion_main!(benches);
