//! Profiling harness for the parallel read path.
//!
//! Run with flamegraph:
//!
//! ```sh
//! cargo flamegraph --example profile_parallel -- [memory|disk] [lz4|none]
//! ```
//!
//! Or plain:
//!
//! ```sh
//! cargo run --release --example profile_parallel -- memory lz4
//! ```

use std::sync::Arc;

use futures::stream::{self, StreamExt};
use tokio::runtime::Runtime;

use array_format::{File, FileConfig, InMemoryStorage, Lz4Codec, NoCompression};

const ARRAY_COUNT: usize = 25_000;
const ELEMENTS_PER_ARRAY: usize = 10_000;
const BLOCK_TARGET: usize = 4 * 1024 * 1024;
const CACHE_SIZE: usize = 256 * 1024 * 1024;
const PARALLELISM: usize = 24;
const CONCURRENCY: usize = 8;

async fn read_parallel_concurrent(
    file: Arc<File>,
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

async fn prepare_in_memory<C: array_format::CompressionCodec + Clone + 'static>(
    codec: C,
) -> (File, Vec<String>, InMemoryStorage) {
    let config = FileConfig {
        block_target_size: BLOCK_TARGET,
        cache_capacity: CACHE_SIZE,
        ..FileConfig::new(codec)
    };
    let mut file = File::create_memory(config).await.unwrap();
    let mut names = Vec::with_capacity(ARRAY_COUNT);
    for i in 0..ARRAY_COUNT {
        let name = format!("arr_{i:05}");
        let values: Vec<i32> = vec![1; ELEMENTS_PER_ARRAY];
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

async fn prepare_on_disk<C: array_format::CompressionCodec + Clone + 'static>(
    path: &std::path::Path,
    codec: C,
) -> Vec<String> {
    let config = FileConfig {
        block_target_size: BLOCK_TARGET,
        cache_capacity: CACHE_SIZE,
        ..FileConfig::new(codec)
    };
    let mut file = File::create(path, config).await.unwrap();
    let mut names = Vec::with_capacity(ARRAY_COUNT);
    for i in 0..ARRAY_COUNT {
        let name = format!("arr_{i:05}");
        let values: Vec<i32> = vec![1; ELEMENTS_PER_ARRAY];
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

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let backend = args.get(1).map(|s| s.as_str()).unwrap_or("memory");
    let codec_arg = args.get(2).map(|s| s.as_str()).unwrap_or("lz4");
    let use_lz4 = codec_arg != "none";

    let rt = Runtime::new().unwrap();

    eprintln!(
        "Preparing {ARRAY_COUNT} arrays ({} each, {backend}, {codec_arg})...",
        humanize(ELEMENTS_PER_ARRAY * 4)
    );

    match backend {
        "memory" => {
            eprintln!("Reading with par={PARALLELISM}, conc={CONCURRENCY}...");
            rt.block_on(async {
                if use_lz4 {
                    let (file, names, _ov) = prepare_in_memory(Lz4Codec).await;
                    let file = Arc::new(file);
                    read_parallel_concurrent(file, names, PARALLELISM, CONCURRENCY).await;
                } else {
                    let (file, names, _ov) = prepare_in_memory(NoCompression).await;
                    let file = Arc::new(file);
                    read_parallel_concurrent(file, names, PARALLELISM, CONCURRENCY).await;
                }
            });
        }
        "disk" => {
            let tmp = tempfile::tempdir().unwrap();
            let path = tmp.path().join("profile.af");

            eprintln!("Reading with par={PARALLELISM}, conc={CONCURRENCY}...");
            rt.block_on(async {
                if use_lz4 {
                    let names = prepare_on_disk(&path, Lz4Codec).await;
                    let cfg = FileConfig {
                        cache_capacity: CACHE_SIZE,
                        ..FileConfig::new(NoCompression)
                    };
                    let file = Arc::new(File::open(&path, cfg).await.unwrap());
                    read_parallel_concurrent(file, names, PARALLELISM, CONCURRENCY).await;
                } else {
                    let names = prepare_on_disk(&path, NoCompression).await;
                    let cfg = FileConfig {
                        cache_capacity: CACHE_SIZE,
                        ..FileConfig::new(NoCompression)
                    };
                    let file = Arc::new(File::open(&path, cfg).await.unwrap());
                    read_parallel_concurrent(file, names, PARALLELISM, CONCURRENCY).await;
                }
            });
        }
        other => {
            eprintln!("Unknown backend: {other}. Use 'memory' or 'disk'.");
            std::process::exit(1);
        }
    }

    let total = ARRAY_COUNT * ELEMENTS_PER_ARRAY * 4;
    eprintln!("Done. Read {} total.", humanize(total));
}

fn humanize(bytes: usize) -> String {
    if bytes >= 1 << 30 {
        format!("{:.1} GiB", bytes as f64 / (1u64 << 30) as f64)
    } else if bytes >= 1 << 20 {
        format!("{:.1} MiB", bytes as f64 / (1u64 << 20) as f64)
    } else {
        format!("{:.1} KiB", bytes as f64 / (1u64 << 10) as f64)
    }
}
