//! Sharing a single `BlockCache` across multiple `ArrayFile`s.
//!
//! Each `ArrayFile` would normally build its own cache. When you open many
//! files, set `ReadConfig::cache` to a pre-built `Arc<BlockCache>` so they
//! share one bounded byte budget. Entries are keyed by `(file_path, block_id)`,
//! so files do not collide.
//!
//! ```sh
//! cargo run --example 09_shared_cache
//! ```

use std::sync::Arc;

use array_format::{ArrayFile, ArrayWriter, BlockCache, Lz4Codec, ReadConfig, WriterConfig};
use ndarray::Array;
use object_store::local::LocalFileSystem;

#[tokio::main]
async fn main() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap())
        as Arc<dyn object_store::ObjectStore>;

    // One cache, shared by every file opened below.
    let shared = Arc::new(BlockCache::new(
        64 * 1024 * 1024, // 64 MiB decompressed block budget
        16 * 1024 * 1024, // 16 MiB raw I/O slab budget
    ));

    let paths = ["a.af", "b.af", "c.af"];

    // Write three files.
    for (i, name) in paths.iter().enumerate() {
        let mut writer = ArrayWriter::new(WriterConfig::new(Lz4Codec));
        writer
            .define_array::<f32>("v", vec!["i".into()], vec![4], None, None)
            .unwrap();
        let data = Array::from_vec(vec![i as f32; 4]).into_dyn();
        writer.write_array("v", vec![0], data.view()).unwrap();
        writer
            .finish(Arc::clone(&store), object_store::path::Path::from(*name))
            .await
            .unwrap();
    }

    // Open all three with the shared cache and read each.
    let mut files = Vec::new();
    for name in &paths {
        let config = ReadConfig {
            cache: Some(Arc::clone(&shared)),
            ..ReadConfig::default()
        };
        files.push(
            ArrayFile::open(
                Arc::clone(&store),
                object_store::path::Path::from(*name),
                config,
            )
            .await
            .unwrap(),
        );
    }

    for (i, file) in files.iter().enumerate() {
        let out = file.read_array::<f32>("v", vec![], vec![]).await.unwrap();
        println!("{} -> {:?}", paths[i], out.as_slice().unwrap());
    }

    // The Arc is reference-counted: one cache, three readers + the local handle.
    println!(
        "shared cache Arc strong count: {}",
        Arc::strong_count(&shared)
    );
    assert_eq!(Arc::strong_count(&shared), 4);
}
