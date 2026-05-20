//! Sharing a single `DeltaCache` across multiple `ArrayFile`s.
//!
//! Each `ArrayFile` would normally build its own cache. When you open many
//! files, set `config.cache` to a pre-built `Arc<DeltaCache>` so they share
//! one bounded byte budget. Entries are keyed by `(file_path, block_id)`,
//! so files do not collide.
//!
//! ```sh
//! cargo run --example 09_shared_cache
//! ```

use std::sync::Arc;

use array_format::{ArrayFile, DeltaCache, FileConfig, Lz4Codec};
use ndarray::Array;
use object_store::local::LocalFileSystem;

#[tokio::main]
async fn main() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap())
        as Arc<dyn object_store::ObjectStore>;

    // One cache, shared by every file opened below.
    let shared = Arc::new(DeltaCache::new(
        64 * 1024 * 1024, // 64 MiB decompressed block budget
        16 * 1024 * 1024, // 16 MiB raw I/O slab budget
    ));

    let paths = ["a.af", "b.af", "c.af"];

    // Write three files.
    for (i, name) in paths.iter().enumerate() {
        let path = object_store::path::Path::from(*name);
        let mut cfg = FileConfig::new(Lz4Codec);
        cfg.cache = Some(Arc::clone(&shared));

        let mut file = ArrayFile::create(Arc::clone(&store), path, cfg).await.unwrap();
        file.define_array::<f32>("v", vec!["i".into()], vec![4], None, None)
            .unwrap();
        let data = Array::from_vec(vec![i as f32; 4]).into_dyn();
        file.write_array("v", vec![0], data.view()).await.unwrap();
        file.flush().await.unwrap();
    }

    // Re-open all three with the shared cache and read each.
    let mut files = Vec::new();
    for name in &paths {
        let path = object_store::path::Path::from(*name);
        let mut cfg = FileConfig::new(Lz4Codec);
        cfg.cache = Some(Arc::clone(&shared));
        files.push(ArrayFile::open(Arc::clone(&store), path, cfg).await.unwrap());
    }

    for (i, file) in files.iter().enumerate() {
        let out = file.read_array::<f32>("v", vec![], vec![]).await.unwrap();
        println!("{} -> {:?}", paths[i], out.as_slice().unwrap());
    }

    // The Arc is reference-counted: one cache, three readers + the local handle.
    println!("shared cache Arc strong count: {}", Arc::strong_count(&shared));
}
