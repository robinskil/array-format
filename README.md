# array-format

`array-format` stores many n-dimensional arrays in a single file. It uses a **delta/overlay architecture**: each flush produces a self-describing sidecar file that stacks on top of the base, recording only the chunks that changed. Reads fall through to older layers for unchanged chunks. Layers can be merged into a single file with `compact`.

## Why this format exists

- Store many arrays in one object or a small set of related files
- Append new arrays and update individual chunks without rewriting the whole file
- Block-level compression (LZ4, Zstd, or none) recorded per block so readers need no configuration
- Chunked or single-chunk layouts with coordinate-addressed reads
- Logical deletes with periodic compaction to reclaim space
- Works with any `object_store`-compatible backend (local filesystem, S3, GCS, Azure)

## Quick start

```rust
use array_format::{File, FileConfig, Lz4Codec};
use ndarray::Array;

// Create a new file
let mut file = File::create(path, FileConfig::new(Lz4Codec)).await?;

// Define and write a 1-D f32 array
file.define_array::<f32>("signal", vec!["t".into()], vec![1024], None, None)?;
let data = Array::from_vec(vec![0.0f32; 1024]).into_dyn();
file.write_array("signal", vec![0], data.view()).await?;

// Commit to a sidecar file ({stem}.1.af)
file.flush().await?;

// Read back — any session after open
let result = file.read_array::<f32>("signal", vec![], vec![]).await?;
```

Variable-length types like `String` and `Vec<u8>` use the same methods:

```rust
file.define_array::<String>("labels", vec!["i".into()], vec![100], None, None)?;
let labels = ndarray::arr1(&["alpha".to_string(), "beta".to_string()]).into_dyn();
file.write_array("labels", vec![0], labels.view()).await?;
let out = file.read_array::<String>("labels", vec![], vec![]).await?;
```

## Delta / overlay architecture

Opening a file discovers the base file and all sidecars:

```text
mydata.af        ← base (overlay_index 0, written by File::create)
mydata.1.af      ← first flush
mydata.2.af      ← second flush
...
```

Each sidecar is fully self-describing: it contains its own block table and array metadata. On read, layers are walked newest-first; the first layer that has a chunk wins. `compact()` merges all layers into a new base and deletes the sidecars.

```text
Before compact:
  mydata.af   mydata.1.af   mydata.2.af

After compact:
  mydata.af   (single merged file)
```

## File API

```rust
// On-disk
File::create(path, config).await?
File::open(path, config).await?           // base + any sidecars

// In-memory (for testing)
File::create_memory(config).await?
file.flush_memory(&storage).await?

// Schema
file.define_array::<T>(name, dim_names, shape, chunk_shape, fill_value)?
file.delete(name)?                        // logical delete (hidden until compact)
file.list_arrays()                        // Vec<MergedArrayMeta>, deleted excluded
file.num_layers()

// ndarray read / write — works for all ArrayElement types
file.write_array(name, start, ndarray_view).await?
file.read_array::<T>(name, start, shape).await?   // vec![], vec![] for full array

// Attributes
file.set_attribute(name, key, AttributeValue::String("m/s".into()))?
file.get_attribute(name, key)?

// Flush and compact
file.flush().await?
file.compact().await?
```

### `FileConfig`

```rust
FileConfig::new(Lz4Codec)                // defaults: 8 MiB blocks, 256 MiB block cache, 64 MiB I/O cache, no shared cache

FileConfig {
    codec: ZstdCodec { level: 3 },
    block_target_size: 8 * 1024 * 1024,
    cache_capacity: 256 * 1024 * 1024,
    io_cache_capacity: 64 * 1024 * 1024,
    cache: None,                         // see "Sharing a cache across files"
}
```

### Sharing a cache across files

By default each `ArrayFile` builds its own `DeltaCache` sized by `cache_capacity` and
`io_cache_capacity`. When you open many files, that adds up. Set `config.cache` to a
pre-built `Arc<DeltaCache>` to put every file under one shared byte budget — entries
are keyed by `(file_path, block_id)`, so files do not interfere with each other.

```rust
use std::sync::Arc;
use array_format::{ArrayFile, DeltaCache, FileConfig, Lz4Codec};

let shared = Arc::new(DeltaCache::new(
    256 * 1024 * 1024,   // decompressed block budget
    64 * 1024 * 1024,    // raw I/O slab budget (0 to disable)
));

let mut cfg = FileConfig::new(Lz4Codec);
cfg.cache = Some(Arc::clone(&shared));

let file_a = ArrayFile::open(store.clone(), path_a, cfg).await?;
// reuse `shared` for file_b, file_c, ... — all bounded by the same budget
```

When `config.cache` is `Some`, the `cache_capacity` / `io_cache_capacity` fields are
ignored for that file.

## Supported data types

All readable and writable types implement the `ArrayElement` trait:

```rust
pub trait ArrayElement: Clone + Send + Sync + 'static {
    const DTYPE: DType;
    fn encode_chunk(values: &[Self]) -> Vec<u8>;
    fn decode_chunk(bytes: &[u8]) -> Vec<Self>;
    fn fill_element(fill: Option<&FillValue>) -> Self;
}
```

### Fixed-width numeric types

Values stored contiguously, little-endian, no per-element headers. Safe zero-copy encode/decode via memcpy.

| Rust type                   | `DType`              |
| --------------------------- | -------------------- |
| `u8`, `u16`, `u32`, `u64`   | `UInt8` … `UInt64`   |
| `i8`, `i16`, `i32`, `i64`   | `Int8` … `Int64`     |
| `f32`, `f64`                | `Float32`, `Float64` |

### Variable-length types

Stored as an offset buffer: `N+1` u32 LE offsets followed by the concatenated payload.

| Rust type | `DType`         |
| --------- | --------------- |
| `String`  | `DType::String` |
| `Vec<u8>` | `DType::Binary` |

Example (`String`, values `["cat", "", "elephant"]`):

```text
offsets (u32 LE): [0, 3, 3, 11]
payload bytes   : 63 61 74  65 6C 65 70 68 61 6E 74
                  c  a  t   e  l  e  p  h  a  n  t
```

## Chunked layout

Defining an array with a `chunk_shape` smaller than the full shape tiles the array into a coordinate grid. Each chunk is stored independently and can be updated without touching others.

```rust
file.define_array::<f32>(
    "grid",
    vec!["x".into(), "y".into()],
    vec![4000, 3000],           // full shape
    Some(vec![1000, 1000]),     // chunk shape → 4×3 grid of chunks
    Some(FillValue::Float(0.0)),
)?;
```

```text
  +--------+--------+--------+
  | (0,0)  | (0,1)  | (0,2)  |
  +--------+--------+--------+
  | (1,0)  | (1,1)  | (1,2)  |
  +--------+--------+--------+
  | (2,0)  | (2,1)  | (2,2)  |
  +--------+--------+--------+
  | (3,0)  | (3,1)  | (3,2)  |
  +--------+--------+--------+
```

`write_array` performs read-modify-write automatically for partial chunk writes. Chunks that haven't been written are filled with the array's fill value on read.

When `chunk_shape` is `None`, the entire array is stored as a single chunk.

## On-disk layout (per delta file)

```text
+-------------------------------+ 0
| Data Region                   |
|  [block 0: compressed bytes]  |
|  [block 1: compressed bytes]  |
|  ...                          |
+-------------------------------+
| Footer (rkyv-serialized)      |
+-------------------------------+
| footer_size  (u64 LE)         |
| magic b"ARRF" (4 bytes)       |
+-------------------------------+ EOF
```

**Footer contents:**

- `version` — format version (currently `1`)
- `overlay_index` — which layer this file represents (`0` = base)
- `base_file_hint` — stem of the base file
- `blocks` — `Vec<BlockMeta>`: id, file offset, compressed/uncompressed sizes, codec
- `arrays` — `Vec<ArrayMeta>`: name, dtype, shape, chunk_shape, chunk coordinates → `ChunkAddress`, fill_value, deleted flag, attributes

**`ChunkAddress`:**

```text
(block_id: u32, offset: u32, size: u32)
```

Find the block by id, decompress, slice `[offset..offset+size]`.

The footer is serialized with `rkyv`. Reading it is a two-pass operation: first read the 12-byte trailer to get `footer_size`, then read the footer payload.

## Storage

```rust
pub trait Storage: Send + Sync {
    fn read_range(&self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>>;
    fn write(&self, data: Bytes) -> BoxFuture<'_, Result<()>>;
    fn size(&self) -> BoxFuture<'_, Result<u64>>;
}
```

Two built-in implementations:

- `ObjectStoreBackend` — wraps any `object_store::ObjectStore`
- `InMemoryStorage` — shared in-memory buffer, useful for testing

## Compression codecs

| Type                  | Notes                 |
| --------------------- | --------------------- |
| `NoCompression`       | Pass-through          |
| `Lz4Codec`            | Fast, via `lz4_flex`  |
| `ZstdCodec { level }` | Level 1–22, default 3 |

The codec is set once in `FileConfig`. Each block records its own codec in the block table, so files can be opened without specifying the codec that was used to write them.

## Deletes and compaction

`file.delete(name)` writes a tombstone to the pending layer. Deleted arrays are excluded from `list_arrays()` and all reads immediately, but their bytes remain on disk until `compact()`.

```rust
file.delete("old_array")?;
file.flush().await?;

// Later: merge all layers into a new base, delete sidecars, reclaim space
file.compact().await?;
assert_eq!(file.num_layers(), 1);
```

## In-memory usage

```rust
use array_format::{File, FileConfig, InMemoryStorage, NoCompression};

let mut file = File::create_memory(FileConfig::new(NoCompression)).await?;
file.define_array::<i32>("data", vec!["x".into()], vec![10], None, None)?;
// ... write ...

let storage = InMemoryStorage::new();
file.flush_memory(&storage).await?;
```
