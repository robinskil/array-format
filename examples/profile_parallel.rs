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

use std::collections::HashMap;
use std::sync::Arc;

use crossbeam_queue::SegQueue;
use futures::stream::{self, StreamExt};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use tokio::runtime::Runtime;

use array_format::{
    BlockId, InMemoryStorage, Lz4Codec, NoCompression, ObjectStoreBackend,
    PrimitiveArray, Reader, Writer, WriterConfig,
};

const ARRAY_COUNT: usize = 25_000;
const ELEMENTS_PER_ARRAY: usize = 10_000;
const BLOCK_TARGET: usize = 4 * 1024 * 1024;
const CACHE_SIZE: u64 = 256 * 1024 * 1024;
const PARALLELISM: usize = 24;
const CONCURRENCY: usize = 8;

fn group_by_block(reader: &Reader) -> Vec<Vec<String>> {
    let mut block_map: HashMap<BlockId, Vec<String>> = HashMap::new();
    for meta in reader.list_arrays() {
        let block_id = match &meta.layout.storage {
            array_format::StorageLayout::Flat { address } => address.block_id,
            array_format::StorageLayout::Chunked { chunks, .. } => chunks.first().unwrap().1.block_id,
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

    let queues: Vec<Arc<SegQueue<Vec<String>>>> = (0..parallelism)
        .map(|_| Arc::new(SegQueue::new()))
        .collect();
    for (i, group) in block_groups.into_iter().enumerate() {
        queues[i % parallelism].push(group);
    }
    let queues = Arc::new(queues);

    let mut handles = Vec::with_capacity(parallelism);
    for task_id in 0..parallelism {
        let reader = Arc::clone(&reader);
        let queues = Arc::clone(&queues);
        handles.push(tokio::spawn(async move {
            loop {
                let own = &queues[task_id];
                let batch = own.pop().or_else(|| {
                    (1..queues.len())
                        .map(|off| (task_id + off) % queues.len())
                        .find_map(|v| queues[v].pop())
                });
                let Some(names) = batch else { break };

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
}

fn write_arrays<C: array_format::CompressionCodec>(writer: &mut Writer<C>) {
    for i in 0..ARRAY_COUNT {
        let name = format!("arr_{i:05}");
        let values: Vec<i32> = vec![1; ELEMENTS_PER_ARRAY];
        let array = PrimitiveArray::from_slice(&values);
        writer.write_array(&name, vec!["x".into()], vec![ELEMENTS_PER_ARRAY as u32], None, &array).unwrap();
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let backend = args.get(1).map(|s| s.as_str()).unwrap_or("memory");
    let codec = args.get(2).map(|s| s.as_str()).unwrap_or("lz4");
    let use_lz4 = codec != "none";

    let rt = Runtime::new().unwrap();

    eprintln!(
        "Preparing {ARRAY_COUNT} arrays ({} each, {backend}, {codec})...",
        humanize(ELEMENTS_PER_ARRAY * 4)
    );

    match backend {
        "memory" => {
            let storage = rt.block_on(async {
                let s = InMemoryStorage::new();
                if use_lz4 {
                    let mut w = Writer::new(
                        s.clone(),
                        WriterConfig {
                            block_target_size: BLOCK_TARGET,
                            codec: Lz4Codec,
                        },
                    );
                    write_arrays(&mut w);
                    w.flush().await.unwrap();
                } else {
                    let mut w = Writer::new(
                        s.clone(),
                        WriterConfig {
                            block_target_size: BLOCK_TARGET,
                            codec: NoCompression,
                        },
                    );
                    write_arrays(&mut w);
                    w.flush().await.unwrap();
                }
                s
            });

            eprintln!("Reading with par={PARALLELISM}, conc={CONCURRENCY}...");
            rt.block_on(async {
                let reader = Arc::new(Reader::open(storage, CACHE_SIZE).await.unwrap());
                read_parallel_concurrent(reader, PARALLELISM, CONCURRENCY).await;
            });
        }
        "disk" => {
            let tmp = tempfile::tempdir().unwrap();
            let filename = "profile.af";

            rt.block_on(async {
                let store = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
                let path = Path::from(filename);
                let backend = ObjectStoreBackend::new(store, path);
                if use_lz4 {
                    let mut w = Writer::new(
                        backend,
                        WriterConfig {
                            block_target_size: BLOCK_TARGET,
                            codec: Lz4Codec,
                        },
                    );
                    write_arrays(&mut w);
                    w.flush().await.unwrap();
                } else {
                    let mut w = Writer::new(
                        backend,
                        WriterConfig {
                            block_target_size: BLOCK_TARGET,
                            codec: NoCompression,
                        },
                    );
                    write_arrays(&mut w);
                    w.flush().await.unwrap();
                }
            });

            eprintln!("Reading with par={PARALLELISM}, conc={CONCURRENCY}...");
            rt.block_on(async {
                let store = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
                let path = Path::from(filename);
                let storage = ObjectStoreBackend::new(store, path);
                let reader = Arc::new(Reader::open(storage, CACHE_SIZE).await.unwrap());
                read_parallel_concurrent(reader, PARALLELISM, CONCURRENCY).await;
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
