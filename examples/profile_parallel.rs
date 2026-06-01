//! Profiling harness for the parallel read path.
//!
//! The file is created on the first run and **reused on subsequent runs**, so
//! flamegraph captures only the read hot-path without write noise.
//!
//! # Usage
//!
//! ```sh
//! # First run: writes profile_data.af then reads it
//! cargo run --release --example profile_parallel -- disk lz4
//!
//! # Subsequent runs: skips writing, reads immediately
//! cargo run --release --example profile_parallel -- disk lz4
//!
//! # Flamegraph (Linux – requires cargo-flamegraph + perf)
//! cargo flamegraph --example profile_parallel -- disk lz4
//!
//! # Custom path and iteration count
//! cargo run --release --example profile_parallel -- disk lz4 --path /tmp/my.af --iters 20
//!
//! # In-memory (recreated every run, useful for CPU-only profiling)
//! cargo flamegraph --example profile_parallel -- memory lz4
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use array_format::{ArrayFile, FileConfig, InMemoryStorage, Lz4Codec, NoCompression};
use futures::stream::{self, StreamExt};
use object_store::{ObjectStore, local::LocalFileSystem};

const ARRAY_COUNT: usize = 25_000;
const ELEMENTS_PER_ARRAY: usize = 10_000;
const BLOCK_TARGET: usize = 4 * 1024 * 1024;
const CACHE_SIZE: usize = 256 * 1024 * 1024;
const PARALLELISM: usize = 24;
const CONCURRENCY: usize = 8;

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

async fn prepare_in_memory<C: array_format::CompressionCodec + Clone + 'static>(
    codec: C,
) -> (ArrayFile, Vec<String>, InMemoryStorage) {
    let config = FileConfig {
        block_target_size: BLOCK_TARGET,
        cache_capacity: CACHE_SIZE,
        ..FileConfig::new(codec)
    };
    let mut file = ArrayFile::create_memory(config).await.unwrap();
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

/// Creates the file at `disk_path / obj_name` if it doesn't already exist.
/// Returns the list of array names.
async fn ensure_on_disk<C: array_format::CompressionCodec + Clone + 'static>(
    store: Arc<dyn ObjectStore>,
    obj_path: object_store::path::Path,
    disk_path: &std::path::Path,
    codec: C,
) -> Vec<String> {
    let names: Vec<String> = (0..ARRAY_COUNT).map(|i| format!("arr_{i:05}")).collect();

    if disk_path.exists() {
        eprintln!("File exists — skipping write phase.");
        return names;
    }

    eprintln!(
        "Creating {} with {ARRAY_COUNT} arrays × {} …",
        disk_path.display(),
        humanize(ELEMENTS_PER_ARRAY * 4),
    );

    let config = FileConfig {
        block_target_size: BLOCK_TARGET,
        cache_capacity: CACHE_SIZE,
        ..FileConfig::new(codec)
    };
    let mut file = ArrayFile::create(Arc::clone(&store), obj_path, config)
        .await
        .unwrap();

    for name in &names {
        let values: Vec<i32> = vec![1; ELEMENTS_PER_ARRAY];
        let nd = ndarray::Array::from_vec(values).into_dyn();
        file.define_array::<i32>(name, vec!["x".into()], vec![ELEMENTS_PER_ARRAY], None, None)
            .unwrap();
        file.write_array(name, vec![0], nd.view()).await.unwrap();
    }
    file.flush().await.unwrap();
    file.compact().await.unwrap();

    eprintln!(
        "Write complete ({}).",
        humanize(ARRAY_COUNT * ELEMENTS_PER_ARRAY * 4)
    );
    names
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let backend = args.get(1).map(|s| s.as_str()).unwrap_or("disk");
    let codec_arg = args.get(2).map(|s| s.as_str()).unwrap_or("lz4");
    let use_lz4 = codec_arg != "none";

    // Parse --path and --iters from remaining args
    let mut custom_path: Option<PathBuf> = None;
    let mut iters: usize = 5;
    let mut idx = 3;
    while idx < args.len() {
        match args[idx].as_str() {
            "--path" => {
                idx += 1;
                custom_path = args.get(idx).map(PathBuf::from);
            }
            "--iters" => {
                idx += 1;
                if let Some(v) = args.get(idx) {
                    iters = v.parse().expect("--iters must be a number");
                }
            }
            _ => {}
        }
        idx += 1;
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(32)
        .build()
        .unwrap();

    eprintln!(
        "backend={backend}  codec={codec_arg}  iters={iters}  par={PARALLELISM}  conc={CONCURRENCY}"
    );

    match backend {
        "memory" => {
            rt.block_on(async {
                eprintln!("Preparing in-memory file…");
                let (file, names, _ov) = if use_lz4 {
                    prepare_in_memory(Lz4Codec).await
                } else {
                    prepare_in_memory(NoCompression).await
                };
                let file = Arc::new(file);
                eprintln!("Starting {iters} read iteration(s)…");
                for i in 1..=iters {
                    eprintln!("  iter {i}/{iters}");
                    read_parallel_concurrent(
                        Arc::clone(&file),
                        names.clone(),
                        PARALLELISM,
                        CONCURRENCY,
                    )
                    .await;
                }
            });
        }

        "disk" => {
            let disk_path = custom_path.unwrap_or_else(|| PathBuf::from("profile_data.af"));
            let filename = disk_path.file_name().unwrap().to_str().unwrap().to_owned();
            let raw_parent = disk_path.parent().unwrap_or(std::path::Path::new("."));
            let raw_parent = if raw_parent.as_os_str().is_empty() {
                std::path::Path::new(".")
            } else {
                raw_parent
            };
            let parent = raw_parent.canonicalize().unwrap();
            let full_path = parent.join(&filename);
            let obj_path = object_store::path::Path::from(filename.as_str());

            let store = Arc::new(LocalFileSystem::new_with_prefix(&parent).unwrap())
                as Arc<dyn ObjectStore>;

            rt.block_on(async {
                let names = if use_lz4 {
                    ensure_on_disk(Arc::clone(&store), obj_path.clone(), &full_path, Lz4Codec).await
                } else {
                    ensure_on_disk(
                        Arc::clone(&store),
                        obj_path.clone(),
                        &full_path,
                        NoCompression,
                    )
                    .await
                };

                let cfg = FileConfig {
                    cache_capacity: CACHE_SIZE,
                    ..FileConfig::new(NoCompression)
                };
                let file = Arc::new(
                    ArrayFile::open(Arc::clone(&store), obj_path, cfg)
                        .await
                        .unwrap(),
                );

                eprintln!("Starting {iters} read iteration(s)…");
                for i in 1..=iters {
                    eprintln!("  iter {i}/{iters}");
                    read_parallel_concurrent(
                        Arc::clone(&file),
                        names.clone(),
                        PARALLELISM,
                        CONCURRENCY,
                    )
                    .await;
                }
            });

            let total = iters * ARRAY_COUNT * ELEMENTS_PER_ARRAY * 4;
            eprintln!("Done. Read {} total.", humanize(total));
        }

        other => {
            eprintln!("Unknown backend: {other}. Use 'memory' or 'disk'.");
            std::process::exit(1);
        }
    }
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
