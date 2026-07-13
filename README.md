# array-format

[![crates.io](https://img.shields.io/crates/v/array-format.svg)](https://crates.io/crates/array-format)
[![docs.rs](https://img.shields.io/docsrs/array-format)](https://docs.rs/array-format)
[![CI](https://github.com/robinskil/array-format/actions/workflows/ci.yml/badge.svg)](https://github.com/robinskil/array-format/actions/workflows/ci.yml)
[![license](https://img.shields.io/crates/l/array-format.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](https://blog.rust-lang.org/)

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
use std::sync::Arc;
use array_format::{ArrayFile, FileConfig, Lz4Codec};
use ndarray::Array;
use object_store::{ObjectStore, local::LocalFileSystem};

// Back the file with any object_store backend
let store = Arc::new(LocalFileSystem::new_with_prefix("/data")?) as Arc<dyn ObjectStore>;
// `path` is the base file (must end in `.af`), not a directory. Sidecars and
// stats are written next to it in the same prefix: signal.1.af, signal.stats, …
let path = object_store::path::Path::from("signal.af");

// Create a new file
let mut file = ArrayFile::create(store, path, FileConfig::new(Lz4Codec)).await?;

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
mydata.af        ← base (overlay_index 0, written by ArrayFile::create)
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
// On-disk — store: Arc<dyn ObjectStore>, path: object_store::path::Path
ArrayFile::create(store, path, config).await?
ArrayFile::open(store, path, config).await?   // base + any sidecars

// In-memory (for testing) — backed by object_store's in-memory backend
ArrayFile::create_memory(config).await?   // then flush()/compact() as normal

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
file.attribute_index(key)                 // Vec<(array_name, Option<&AttributeValue>)> — one attribute across all arrays, for pruning

// Statistics (min/max/null_count/row_count, refreshed on flush & compact)
file.array_stats(name)                    // Option<&ArrayStats>

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

- `version` — format version (currently `5`)
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

Files are read and written through any
[`object_store`](https://docs.rs/object_store) backend — local filesystem, S3,
GCS, Azure, or its in-memory backend. Pass the `Arc<dyn ObjectStore>` and a base
`path` (ending in `.af`) to `ArrayFile::create` / `ArrayFile::open`; array-format
manages the base file, sidecars, and stats file within that store. There is no
separate storage trait to implement — in-memory use goes through
[`create_memory`](#in-memory-usage), which uses `object_store`'s in-memory backend.

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

## Attributes

Each array carries user-defined key-value attributes (units, scale factors, provenance, …). Set and read them per array:

```rust
file.set_attribute("pressure", "units", AttributeValue::String("hPa".into()))?;
file.get_attribute("pressure", "units")?;   // Option<&AttributeValue>
```

An `AttributeValue` is a scalar (`Bool`, the sized `Int*`/`UInt*`, `Float32`/`Float64`, `String`), raw `Binary(Vec<u8>)`, or a typed list of any of those (`Int32List`, `Float64List`, `StringList`, `BinaryList`, …):

```rust
file.set_attribute("pressure", "checksum", AttributeValue::Binary(vec![0xde, 0xad]))?;
file.set_attribute("pressure", "valid_range", AttributeValue::Float32List(vec![0.0, 1100.0]))?;
```

Attributes live in the footer dictionaries and are fully in memory once the file is open, so there is no sidecar to maintain. To prune by attribute, `attribute_index` returns one attribute across every visible array in a single call — a full column with `None` where the attribute is absent — instead of walking arrays one by one:

```rust
// Select the arrays measured in hPa without a per-array loop.
let hpa: Vec<String> = file
    .attribute_index("units")               // Vec<(String, Option<&AttributeValue>)>
    .into_iter()
    .filter(|(_, v)| matches!(v, Some(AttributeValue::String(s)) if s == "hPa"))
    .map(|(name, _)| name)
    .collect();
```

Logically deleted arrays are omitted from the result.

### File-level metadata

Attributes attach to arrays, so to describe the *file* as a whole (title, provenance, schema version) — including a metadata-only file with no data — define a scalar placeholder array with an empty shape and hang the attributes on it. Nothing is ever written to it, so `flush` skips stats and it costs almost nothing:

```rust
file.define_array::<u8>("__file__", vec![], vec![], None, None)?;   // empty shape → no data
file.set_attribute("__file__", "title", AttributeValue::String("My Dataset".into()))?;
file.flush().await?;
```

The placeholder appears in `list_arrays()` like any array; filter out its name to show only real data arrays. See `examples/10_file_metadata.rs`.

## Statistics

Every array carries aggregate statistics, recomputed automatically on `flush()` and `compact()` and persisted to a `{stem}.stats` sidecar (same rkyv + trailer format as the footer, magic `b"ARST"`). On `open()` they are loaded if present; a missing or unreadable stats file is not an error — `array_stats` simply returns `None` until the next flush.

```rust
file.flush().await?;                       // computes & writes {stem}.stats

if let Some(s) = file.array_stats("signal") {
    println!("{:?} .. {:?}", s.min, s.max);
    println!("{} of {} are fill/unwritten", s.null_count, s.row_count);
}
```

`ArrayStats` covers all chunks of one array:

| Field         | Meaning                                                                 |
| ------------- | ----------------------------------------------------------------------- |
| `name`        | Array name                                                              |
| `min` / `max` | Global min/max across all chunks; `None` for dtypes without ordering    |
| `null_count`  | Elements equal to the fill value, including positions never written     |
| `row_count`   | Total element count across all chunks (the product of the array shape)  |

`min`/`max` are typed via `StatValue`, which mirrors the dtype families:

```rust
pub enum StatValue {
    Int(i64),
    UInt(u64),
    Float(f64),
    Bytes(Vec<u8>),     // String / Binary, compared lexicographically
    TimestampNs(i64),
}
```

Stats are computed incrementally: a flush only recomputes arrays whose chunks were dirtied in that flush and merges them with the previously stored stats, so unchanged arrays are not re-scanned.

## In-memory usage

`create_memory` backs the file with `object_store`'s in-memory backend, so it
behaves exactly like an on-disk file — same `flush`/`compact` — but keeps
everything in process. Handy for tests and ephemeral pipelines.

```rust
use array_format::{ArrayFile, FileConfig, NoCompression};

let mut file = ArrayFile::create_memory(FileConfig::new(NoCompression)).await?;
file.define_array::<i32>("data", vec!["x".into()], vec![10], None, None)?;
// ... write ...
file.flush().await?;
```

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
