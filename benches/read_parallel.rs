//! Benchmarks for parallel and concurrent reads of thousands of arrays.

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::stream::{self, StreamExt};
use rand::Rng;

use array_format::{ArrayFile, FileConfig, InMemoryStorage, Lz4Codec, NoCompression};
use object_store::{ObjectStore, local::LocalFileSystem};

const MANY_ARRAYS_COUNT: usize = 25_000;
const ELEMENTS_PER_ARRAY: usize = 10_000;
const BLOCK_TARGET: usize = 8 * 1024 * 1024;
const CACHE_SIZE: usize = 512 * 1024 * 1024;

fn total_bytes() -> u64 {
    (MANY_ARRAYS_COUNT * ELEMENTS_PER_ARRAY * std::mem::size_of::<i32>()) as u64
}

async fn prepare_many_arrays_in_memory<C: array_format::CompressionCodec + Clone + 'static>(
    codec: C,
) -> (ArrayFile, Vec<String>, InMemoryStorage) {
    let config = FileConfig {
        block_target_size: BLOCK_TARGET,
        cache_capacity: CACHE_SIZE,
        ..FileConfig::new(codec)
    };
    let mut file = ArrayFile::create_memory(config).await.unwrap();

    let mut rng = rand::rng();
    let mut names = Vec::with_capacity(MANY_ARRAYS_COUNT);
    for i in 0..MANY_ARRAYS_COUNT {
        let name = format!("arr_{i:05}");
        let values: Vec<i32> = (0..ELEMENTS_PER_ARRAY)
            .map(|_| rng.random_range(0..10))
            .collect();
        let nd = ndarray::Array::from_vec(values).into_dyn();
        file.define_array::<i32>(
            &name,
            vec!["x".into()],
            vec![ELEMENTS_PER_ARRAY],
            None,
            None,
        )
        .unwrap();
        file.write_array(&name, vec![0], nd.view()).await.unwrap();
        names.push(name);
    }
    let overlay = InMemoryStorage::new();
    file.flush_memory(&overlay).await.unwrap();
    (file, names, overlay)
}

async fn prepare_many_arrays_on_disk<C: array_format::CompressionCodec + Clone + 'static>(
    store: Arc<dyn ObjectStore>,
    path: object_store::path::Path,
    codec: C,
) -> Vec<String> {
    let config = FileConfig {
        block_target_size: BLOCK_TARGET,
        cache_capacity: CACHE_SIZE,
        ..FileConfig::new(codec)
    };
    let mut file = ArrayFile::create(Arc::clone(&store), path, config)
        .await
        .unwrap();

    let mut rng = rand::rng();
    let mut names = Vec::with_capacity(MANY_ARRAYS_COUNT);
    for i in 0..MANY_ARRAYS_COUNT {
        let name = format!("arr_{i:05}");
        let values: Vec<i32> = (0..ELEMENTS_PER_ARRAY)
            .map(|_| rng.random_range(0..10))
            .collect();
        let nd = ndarray::Array::from_vec(values).into_dyn();
        file.define_array::<i32>(
            &name,
            vec!["x".into()],
            vec![ELEMENTS_PER_ARRAY],
            None,
            None,
        )
        .unwrap();
        file.write_array(&name, vec![0], nd.view()).await.unwrap();
        names.push(name);
    }
    file.flush().await.unwrap();
    file.compact().await.unwrap();
    names
}

async fn read_sequential(file: &ArrayFile, names: &[String]) {
    for name in names {
        file.read_array::<i32>(name, vec![], vec![]).await.unwrap();
    }
}

async fn read_parallel_concurrent(
    file: Arc<ArrayFile>,
    names: Vec<String>,
    parallelism: usize,
    concurrency: usize,
) {
    let mut partitions: Vec<Vec<String>> = (0..parallelism).map(|_| Vec::new()).collect();
    for (i, name) in names.into_iter().enumerate() {
        partitions[i % parallelism].push(name);
    }

    let mut handles = Vec::with_capacity(parallelism);
    for partition in partitions {
        let file = Arc::clone(&file);
        handles.push(tokio::spawn(async move {
            stream::iter(partition)
                .map(|name| {
                    let file = Arc::clone(&file);
                    async move {
                        file.read_array::<i32>(&name, vec![], vec![]).await.unwrap();
                    }
                })
                .buffered(concurrency)
                .for_each(|_| async {})
                .await;
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

fn bench_many_arrays_memory(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(32)
        .build()
        .unwrap();

    let mut group = c.benchmark_group("many_arrays_memory");
    group.throughput(Throughput::Bytes(total_bytes()));
    group.sample_size(10);

    let (file_lz4, names_lz4, _ov) = rt.block_on(prepare_many_arrays_in_memory(Lz4Codec));
    let file_lz4 = Arc::new(file_lz4);

    group.bench_function(BenchmarkId::new("lz4", "sequential"), |b| {
        let file = Arc::clone(&file_lz4);
        let names = names_lz4.clone();
        b.to_async(&rt).iter(|| {
            let file = Arc::clone(&file);
            let names = names.clone();
            async move {
                read_sequential(&file, &names).await;
            }
        })
    });

    {
        let (par, conc) = (32usize, 32usize);
        group.bench_function(
            BenchmarkId::new("lz4", format!("par{par}_conc{conc}")),
            |b| {
                let file = Arc::clone(&file_lz4);
                let names = names_lz4.clone();
                b.to_async(&rt).iter(|| {
                    let file = Arc::clone(&file);
                    let names = names.clone();
                    async move {
                        read_parallel_concurrent(file, names, par, conc).await;
                    }
                })
            },
        );
    }

    let (file_none, names_none, _ov) = rt.block_on(prepare_many_arrays_in_memory(NoCompression));
    let file_none = Arc::new(file_none);

    group.bench_function(BenchmarkId::new("none", "sequential"), |b| {
        let file = Arc::clone(&file_none);
        let names = names_none.clone();
        b.to_async(&rt).iter(|| {
            let file = Arc::clone(&file);
            let names = names.clone();
            async move {
                read_sequential(&file, &names).await;
            }
        })
    });

    {
        let (par, conc) = (32usize, 32usize);
        group.bench_function(
            BenchmarkId::new("none", format!("par{par}_conc{conc}")),
            |b| {
                let file = Arc::clone(&file_none);
                let names = names_none.clone();
                b.to_async(&rt).iter(|| {
                    let file = Arc::clone(&file);
                    let names = names.clone();
                    async move {
                        read_parallel_concurrent(file, names, par, conc).await;
                    }
                })
            },
        );
    }

    group.finish();
}

fn bench_many_arrays_file(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(32)
        .build()
        .unwrap();
    let tmp_dir = tempfile::tempdir().unwrap();
    let store =
        Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path()).unwrap()) as Arc<dyn ObjectStore>;

    let mut group = c.benchmark_group("many_arrays_file");
    group.throughput(Throughput::Bytes(total_bytes()));
    group.sample_size(10);

    let path_lz4 = object_store::path::Path::from("many_lz4.af");
    let names_lz4 = rt.block_on(prepare_many_arrays_on_disk(
        Arc::clone(&store),
        path_lz4.clone(),
        Lz4Codec,
    ));
    let file_lz4 = rt.block_on(async {
        Arc::new(
            ArrayFile::open(
                Arc::clone(&store),
                path_lz4,
                FileConfig {
                    cache_capacity: CACHE_SIZE,
                    ..FileConfig::new(NoCompression)
                },
            )
            .await
            .unwrap(),
        )
    });

    {
        let (par, conc) = (32usize, 32usize);
        group.bench_function(
            BenchmarkId::new("lz4", format!("par{par}_conc{conc}")),
            |b| {
                let file = Arc::clone(&file_lz4);
                let names = names_lz4.clone();
                b.to_async(&rt).iter(|| {
                    let file = Arc::clone(&file);
                    let names = names.clone();
                    async move {
                        read_parallel_concurrent(file, names, par, conc).await;
                    }
                })
            },
        );
    }

    let path_none = object_store::path::Path::from("many_none.af");
    let names_none = rt.block_on(prepare_many_arrays_on_disk(
        Arc::clone(&store),
        path_none.clone(),
        NoCompression,
    ));
    let file_none = rt.block_on(async {
        Arc::new(
            ArrayFile::open(
                Arc::clone(&store),
                path_none,
                FileConfig {
                    cache_capacity: CACHE_SIZE,
                    ..FileConfig::new(NoCompression)
                },
            )
            .await
            .unwrap(),
        )
    });

    group.bench_function(BenchmarkId::new("none", "sequential"), |b| {
        let file = Arc::clone(&file_none);
        let names = names_none.clone();
        b.to_async(&rt).iter(|| {
            let file = Arc::clone(&file);
            let names = names.clone();
            async move {
                read_sequential(&file, &names).await;
            }
        })
    });

    {
        let (par, conc) = (32usize, 32usize);
        group.bench_function(
            BenchmarkId::new("none", format!("par{par}_conc{conc}")),
            |b| {
                let file = Arc::clone(&file_none);
                let names = names_none.clone();
                b.to_async(&rt).iter(|| {
                    let file = Arc::clone(&file);
                    let names = names.clone();
                    async move {
                        read_parallel_concurrent(file, names, par, conc).await;
                    }
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_many_arrays_memory, bench_many_arrays_file);
criterion_main!(benches);
