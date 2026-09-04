# array-format

[![crates.io](https://img.shields.io/crates/v/array-format.svg)](https://crates.io/crates/array-format)
[![docs.rs](https://img.shields.io/docsrs/array-format)](https://docs.rs/array-format)
[![CI](https://github.com/robinskil/array-format/actions/workflows/ci.yml/badge.svg)](https://github.com/robinskil/array-format/actions/workflows/ci.yml)
[![license](https://img.shields.io/crates/l/array-format.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](https://blog.rust-lang.org/)

`array-format` stores many n-dimensional arrays in a single **immutable** file. An `ArrayWriter` builds the file and writes it once. An `ArrayFile` opens it read-only, with every array's metadata, attributes and statistics in memory from the first moment. To change a file, copy what you keep into a new writer and finish to a new path.

## Why this format exists

- Store many arrays in one object, on any `object_store` backend (local filesystem, S3, GCS, Azure)
- Block-level compression (LZ4, Zstd, or none) recorded per block, so readers need no configuration
- Chunked or single-chunk layouts with coordinate-addressed reads
- Per-array key-value attributes, in memory on open; one attribute across every array is a single call
- Per-array statistics (min, max, null count, row count) computed while writing and stored in the footer
- One file, one footer, no sidecars, no layers to merge

## Quick start

```rust
use std::sync::Arc;
use array_format::{ArrayFile, ArrayWriter, Lz4Codec, ReadConfig, WriterConfig};
use ndarray::Array;
use object_store::{ObjectStore, local::LocalFileSystem};

let store = Arc::new(LocalFileSystem::new_with_prefix("/data")?) as Arc<dyn ObjectStore>;
let path = object_store::path::Path::from("signal.af");

// Build the file: define arrays, write data, set attributes.
let mut writer = ArrayWriter::new(WriterConfig::new(Lz4Codec));
writer.define_array::<f32>("signal", vec!["t".into()], vec![1024], None, None)?;
let data = Array::from_vec(vec![0.0f32; 1024]).into_dyn();
writer.write_array("signal", vec![0], data.view())?;

// Write it once. `finish` returns the file open for reading.
let file = writer.finish(Arc::clone(&store), path.clone()).await?;
let result = file.read_array::<f32>("signal", vec![], vec![]).await?;

// Any later session opens it read-only. No codec is needed to open a file.
let file = ArrayFile::open(store, path, ReadConfig::default()).await?;
```

Variable-length types like `String` and `Vec<u8>` use the same methods:

```rust
writer.define_array::<String>("labels", vec!["i".into()], vec![100], None, None)?;
let labels = ndarray::arr1(&["alpha".to_string(), "beta".to_string()]).into_dyn();
writer.write_array("labels", vec![0], labels.view())?;
// ...
let out = file.read_array::<String>("labels", vec![], vec![]).await?;
```

## Immutable files

A finished file never changes. That is what makes the reader simple: one footer, one block table, one chunk table per array, and no state to reconcile.

To change a file, rewrite it. `copy_array` moves one array's definition, written chunks and attributes from an open file into a writer:

```rust
let source = ArrayFile::open(Arc::clone(&store), old_path, ReadConfig::default()).await?;

let mut writer = ArrayWriter::new(WriterConfig::new(Lz4Codec));
for info in source.arrays() {
    if info.name != "obsolete" {                      // drop an array
        writer.copy_array(&source, &info.name).await?;
    }
}
writer.write_array("signal", vec![10], patch.view())?; // patch a copied array
writer.define_array::<u8>("flags", vec!["t".into()], vec![1024], None, None)?; // add one

let edited = writer.finish(store, new_path).await?;    // the original is untouched
```

Copied chunks are read decompressed and packed into the new writer's blocks, so a rewrite can also change the codec or block size. Statistics are recomputed from the copied bytes.

## File API

```rust
// Writer
let mut writer = ArrayWriter::new(WriterConfig::new(codec));
writer.define_array::<T>(name, dim_names, shape, chunk_shape, fill_value)?
writer.write_array(name, start, ndarray_view)?          // sync; partial chunks are read-modify-written
writer.set_attribute(name, key, AttributeValue::String("m/s".into()))?
writer.copy_array(&source_file, name).await?            // from an open ArrayFile
let file = writer.finish(store, path).await?            // writes once, returns the open file

// Reader — store: Arc<dyn ObjectStore>, path: object_store::path::Path
let file = ArrayFile::open(store, path, ReadConfig::default()).await?;
file.arrays()                       // &[ArrayInfo], file order, borrowed
file.array(name)                    // Option<&ArrayInfo>
file.read_array::<T>(name, start, shape).await?   // vec![], vec![] for the whole array

// Attributes (in memory from open)
file.get_attribute(name, key)       // Option<&AttributeValue>
file.attributes(name)               // Option<&HashMap<EcoString, AttributeValue>>
file.attribute_index(key)           // Vec<(&str, Option<&AttributeValue>)>, one attribute across all arrays

// Statistics (in the footer, available on open)
file.array_stats(name)              // Option<&ArrayStats>
file.stats()                        // iterator over every array's ArrayStats
```

### `WriterConfig` and `ReadConfig`

```rust
WriterConfig::new(Lz4Codec)          // default: 8 MiB blocks
WriterConfig {
    codec: ZstdCodec { level: 3 },
    block_target_size: 8 * 1024 * 1024,
}

ReadConfig::default()                // 256 MiB block cache, 64 MiB I/O cache, no shared cache
ReadConfig {
    cache_capacity: 256 * 1024 * 1024,
    io_cache_capacity: 64 * 1024 * 1024,
    cache: None,                     // see "Sharing a cache across files"
}
```

A reader needs no codec. Every block records its own.

### Sharing a cache across files

By default each `ArrayFile` builds its own `BlockCache` sized by `cache_capacity` and
`io_cache_capacity`. When you open many files, that adds up. Set `ReadConfig::cache` to a
pre-built `Arc<BlockCache>` to put every file under one shared byte budget. Entries are
keyed by `(file_path, block_id)`, so files do not interfere with each other.

```rust
use std::sync::Arc;
use array_format::{ArrayFile, BlockCache, ReadConfig};

let shared = Arc::new(BlockCache::new(
    256 * 1024 * 1024,   // decompressed block budget
    64 * 1024 * 1024,    // raw I/O slab budget (0 to disable)
));

let config = ReadConfig { cache: Some(Arc::clone(&shared)), ..ReadConfig::default() };
let file_a = ArrayFile::open(store.clone(), path_a, config).await?;
// reuse `shared` for file_b, file_c, ... — all bounded by the same budget
```

When `cache` is `Some`, the two capacity fields are ignored for that file.

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
| `TimestampNs`               | `TimestampNs`        |

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

Defining an array with a `chunk_shape` smaller than the full shape tiles the array into a coordinate grid. Each chunk is stored independently, and a read touches only the chunks that overlap the requested region.

```rust
writer.define_array::<f32>(
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

`write_array` need not be chunk-aligned. A chunk the region only partly covers is read back from the writer's own blocks, patched and written again. Chunks that are never written are not stored; they read as the array's fill value.

When `chunk_shape` is `None`, the entire array is stored as a single chunk.

### Zero-length dimensions

An axis may have length 0. The array then holds no elements. NetCDF declares such a dimension when the records it holds are absent, for example `N_HISTORY` in an Argo profile.

```rust
writer.define_array::<f32>(
    "history",
    vec!["n_history".into(), "x".into()],
    vec![0, 3],                 // an empty axis
    None,
    None,
)?;
```

`write_array` with an empty view writes nothing and returns `Ok`. `read_array` returns an empty array of shape `[0, 3]`. A chunk extent of 0 on such an axis is stored as 1, because an empty axis yields no chunks. A chunk extent of 0 on a non-empty axis is rejected with `Error::InvalidChunkShape`.

## On-disk layout

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

- `version` — format version (currently `6`; files from version 5 and below are rejected)
- `blocks` — `Vec<BlockMeta>`: id, file offset, compressed/uncompressed sizes, codec. Ids are dense, so `blocks[id]` is the block.
- `strings` — the string pool: every attribute key and string value, stored once
- `arrays` — `Vec<ArrayMeta>`, in definition order: name, dtype, shape, dimension names, chunk shape, fill value, the chunk table (coordinate → `ChunkAddress`, sorted by coordinate), attributes as `(key index, value)` pairs, and statistics

Only strings are interned. An attribute value's `String` and `StringList` variants hold indices into the pool; every other value is stored inline.

**`ChunkAddress`:**

```text
(block_id: u32, offset: u32, size: u32)
```

Find the block by id, decompress, slice `[offset..offset+size]`. The chunk table is sorted, so a coordinate is found by binary search.

The footer is serialized with `rkyv`. Reading it is a two-pass operation: first read the 12-byte trailer to get `footer_size`, then read the footer payload. Nothing else is read until a chunk is requested.

## Storage

Files are read and written through any
[`object_store`](https://docs.rs/object_store) backend — local filesystem, S3,
GCS, Azure, or its in-memory backend. Pass the `Arc<dyn ObjectStore>` and a
`path` to `ArrayWriter::finish` and `ArrayFile::open`. A file is one object;
there are no sidecars. `finish` streams the data region and the footer to the
store in one pass without holding the whole file in memory.

## Compression codecs

| Type                  | Notes                 |
| --------------------- | --------------------- |
| `NoCompression`       | Pass-through          |
| `Lz4Codec`            | Fast, via `lz4_flex`  |
| `ZstdCodec { level }` | Level 1–22, default 3 |

The codec is set once in `WriterConfig`. Each block records its own codec in the block table, so a file opens without any knowledge of how it was written.

## Attributes

Each array carries user-defined key-value attributes (units, scale factors, provenance, …). Set them on the writer, read them from the file:

```rust
writer.set_attribute("pressure", "units", AttributeValue::String("hPa".into()))?;
// ...
file.get_attribute("pressure", "units");   // Option<&AttributeValue>
file.attributes("pressure");               // Option<&HashMap<EcoString, AttributeValue>>
```

An `AttributeValue` is a scalar (`Bool`, the sized `Int*`/`UInt*`, `Float32`/`Float64`, `String`), raw `Binary`, or a typed list of any of those (`Int32List`, `Float64List`, `StringList`, `BinaryList`, …). Strings are `EcoString` and lists are boxed slices:

```rust
writer.set_attribute("pressure", "checksum", AttributeValue::Binary(Box::new([0xde, 0xad])))?;
writer.set_attribute("pressure", "valid_range", AttributeValue::Float32List(Box::new([0.0, 1100.0])))?;
```

Attributes are in memory from the moment a file opens: one map per array, built straight from the footer. `get_attribute` is two hash lookups. `attribute_index` returns one attribute across every array in a single pass — a full column with `None` where the attribute is absent — so you can select arrays by attribute without a call per array:

```rust
// Select the arrays measured in hPa.
let hpa: Vec<&str> = file
    .attribute_index("units")               // Vec<(&str, Option<&AttributeValue>)>
    .into_iter()
    .filter(|(_, v)| matches!(v, Some(AttributeValue::String(s)) if s == "hPa"))
    .map(|(name, _)| name)
    .collect();
```

Names and values borrow from the open file; the call allocates one vector.

The memory layout is deliberate. `EcoString` is 16 bytes and keeps up to 15 bytes inline, so a key like `units` or a value like `m/s` costs no allocation. Longer strings are one allocation shared by every array that carries the same value, because the on-disk pool stores each string once and the reader hands out clones. Lists are `Box<[T]>`, 16 bytes, which keeps the whole `AttributeValue` at 24 bytes.

### File-level metadata

Attributes attach to arrays, so to describe the *file* as a whole (title, provenance, schema version) — including a metadata-only file with no data — define a scalar placeholder array with an empty shape and hang the attributes on it. Nothing is written to it, so it costs its attributes and a few bytes of metadata:

```rust
writer.define_array::<u8>("__file__", vec![], vec![], None, None)?;   // empty shape → no data
writer.set_attribute("__file__", "title", AttributeValue::String("My Dataset".into()))?;
```

The placeholder appears in `arrays()` like any array; filter out its name to show only real data arrays. See `examples/10_file_metadata.rs`.

## Statistics

Every array carries aggregate statistics. The writer computes a partial (min, max, null count) for each chunk as it is written; a chunk written twice keeps only the last partial. `finish` merges the partials per array and stores the result in the footer, so statistics cost no second pass over the data and are available the moment a file opens.

```rust
if let Some(s) = file.array_stats("signal") {
    println!("{:?} .. {:?}", s.min, s.max);
    println!("{} of {} are fill/unwritten", s.null_count, s.row_count);
}
for s in file.stats() { /* every array, in file order */ }
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

## In-memory usage

`object_store`'s in-memory backend makes a file that behaves exactly like one on disk but never leaves the process. Handy for tests and ephemeral pipelines.

```rust
use std::sync::Arc;
use array_format::{ArrayWriter, NoCompression, WriterConfig};
use object_store::memory::InMemory;

let mut writer = ArrayWriter::new(WriterConfig::new(NoCompression));
writer.define_array::<i32>("data", vec!["x".into()], vec![10], None, None)?;
// ... write ...
let file = writer
    .finish(Arc::new(InMemory::new()), object_store::path::Path::from("data.af"))
    .await?;
```

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
