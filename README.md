# array-format

`array-format` stores many n-dimensional arrays in one file.

Data bytes are written first into blocks and a footer index is appended at the end of the file. The footer contains all metadata needed to reconstruct arrays and locate their bytes. All I/O goes through a `Storage` trait backed by the `object_store` crate, so any backend (local filesystem, S3, GCS, Azure) works out of the box.

## Why this format exists

This format is designed for workloads where you want:

- multiple arrays in one object/file
- either whole-array (`flat`) or chunked storage
- append-friendly writes
- efficient lookup via a single footer read
- block-level compression with per-block codec metadata
- parallel-safe, cache-backed reads with zero-copy slicing
- logical deletes followed by periodic compaction

## Core concepts

### Arrays

Each stored array has a definition in the footer:

- **name** — unique string identifier
- **dtype** — data type (`Int32`, `Float64`, `String`, `List { child }`, …)
- **dimensions** — ordered list of dimension names (e.g. `["x", "y", "time"]`)
- **layout** — `Flat` (single address) or `Chunked` (coordinate → address map)
- **deleted** — logical delete flag

Typed access is provided through concrete array types (`PrimitiveArray<T>`, `BinaryArray`, `StringArray`) that implement the `ArrayData` trait.

### Blocks

Array bytes are packed into blocks. A block has a configurable target size (default: 8 MiB).

- one block can hold bytes from multiple arrays/chunks
- a flat array is never split across blocks — it lives at a single address
- data within a block is 8-byte aligned so typed slices can be constructed zero-copy
- each block is compressed independently with the configured codec
- the codec used is recorded per block in the block table, so the reader can decompress without knowing the writer's configuration

### Addresses

An array or chunk is located via a `ChunkAddress`:

```text
(block_id, offset, size)
```

This means: find the decompressed block with id `block_id`, skip `offset` bytes, and read `size` bytes.

### Footer

The footer is serialized with `rkyv` and appended after the data region. A 12-byte trailer at the very end of the file stores the footer payload size (u64 LE) followed by a 4-byte magic number (`ARRF`).

The footer contains:

- **version** — format version (currently `1`)
- **block table** — `Vec<BlockMeta>`, one entry per block:
  - `id: BlockId`
  - `file_offset: u64` — byte position in the file
  - `compressed_size: u64`
  - `uncompressed_size: u64`
  - `codec: CodecId` — `None` or `Named("lz4")`, `Named("zstd")`, etc.
- **array table** — `Vec<ArrayMeta>`, one entry per array:
  - `name`, `dtype`, `dimensions`
  - `layout: ArrayLayout` — flat or chunked with chunk coordinates
  - `deleted: bool`

Reading the footer is a two-pass operation: first read the 12-byte trailer to learn the footer size, then read the full footer payload.

## Supported data types

```text
DType
├── Bool            1 byte (0 or 1)
├── Int8            1 byte
├── Int16           2 bytes LE
├── Int32           4 bytes LE
├── Int64           8 bytes LE
├── UInt8           1 byte
├── UInt16          2 bytes LE
├── UInt32          4 bytes LE
├── UInt64          8 bytes LE
├── Float32         4 bytes IEEE 754 LE
├── Float64         8 bytes IEEE 754 LE
├── String          variable-length UTF-8
├── Binary          variable-length bytes
├── FixedSizeList   { child: DType, size: u32 }
└── List            { child: DType }
```

### Fixed-width primitives

Values are stored contiguously with no per-element header. Integer and float values are little-endian. Element size is constant and determined by the dtype.

Example (`Int16`, values `[1, 2, 300]`):

```text
01 00 02 00 2C 01
```

### Variable-length values (String, Binary)

Stored as two logical buffers:

- offsets buffer — `N + 1` u32 offsets (little-endian)
- values buffer — concatenated payload bytes

Example (`String`, values `["cat", "", "elephant"]`):

```text
offsets (u32): [0, 3, 3, 11]
values bytes : [63 61 74 65 6C 65 70 68 61 6E 74]

index 0 => bytes[0..3]  = "cat"
index 1 => bytes[3..3]  = ""
index 2 => bytes[3..11] = "elephant"
```

### Fixed-size lists

`FixedSizeList { child, size }` — each element is a list of exactly `size` child values. No offsets buffer needed.

Example (`FixedSizeList { child: Int8, size: 3 }`, values `[[1,2,3], [4,5,6]]`):

```text
child values: [01 02 03 04 05 06]
```

### Variable-length lists

`List { child }` — each element can contain a different number of child values.

- parent offsets buffer — `N + 1` u32 entries
- child values buffer — all child elements concatenated

Example (`List { child: Int16 }`, values `[[10, 20, 30], [], [40]]`):

```text
parent offsets (u32): [0, 3, 3, 4]
child values (int16 LE): [0A 00 14 00 1E 00 28 00]
```

Nesting is supported — each variable-length level introduces its own offsets buffer.

### Nullability

Not supported. Validity bitmaps are not stored. Empty strings, empty binaries, and empty lists are valid values but are not null.

## On-disk layout

```text
+-------------------------------+ 0
| Data Region                   |
|  [block 0 bytes]              |
|  [block 1 bytes]              |
|  ...                          |
+-------------------------------+
| Footer (rkyv-serialized)      |
+-------------------------------+
| footer_size (u64 LE)          |
| magic b"ARRF" (4 bytes)      |
+-------------------------------+ EOF
```

Inside a block, payloads are packed with 8-byte alignment padding:

```text
block 5 (example, uncompressed view)

+--[arrA flat]--+--[pad]--+--[arrB chunk 3]--+--[pad]--+--[arrC flat]--+
0               ^         ^                   ^         ^               ^
            8-aligned   8-aligned          8-aligned  8-aligned
```

## Flat vs chunked layout

### Flat

A flat array is stored as one contiguous payload within a single block. The footer records a single `ChunkAddress`:

```text
Array X (flat)
  address: (block_id=1, offset=0, size=40000)
```

### Chunked

A chunked array is divided into a coordinate grid. Each chunk has its own `ChunkAddress` and may reside in any block.

```text
Array Y (chunked, 2D, chunk_shape=[1000, 1000])

  +--------+--------+
  | (0,0)  | (0,1)  |
  +--------+--------+
  | (1,0)  | (1,1)  |
  +--------+--------+

Footer mapping:
  (0,0) -> (block=3, offset=0,    size=1MB)
  (0,1) -> (block=7, offset=2MB,  size=1MB)
  (1,0) -> (block=3, offset=1MB,  size=1MB)
  (1,1) -> (block=9, offset=0,    size=1MB)
```

## Storage

All I/O goes through the `Storage` trait:

```rust
pub trait Storage: Send + Sync {
    fn read_range(&self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>>;
    fn write(&self, data: Bytes) -> BoxFuture<'_, Result<()>>;
    fn size(&self) -> BoxFuture<'_, Result<u64>>;
}
```

The trait is dyn-compatible (uses `BoxFuture`). Two implementations are provided:

- **`ObjectStoreBackend`** — wraps any `object_store::ObjectStore` (local filesystem, S3, GCS, Azure)
- **`InMemoryStorage`** — in-memory buffer for testing

## Read path

Opening a reader parses the footer and builds O(1) lookup indexes for arrays (by name) and blocks (by id). The reader owns a shared `moka`-based block cache.

To read an array:

1. Look up `ArrayMeta` by name (HashMap, O(1)).
2. Determine `ChunkAddress`(es) from the layout.
3. For each referenced block, consult the cache:
   - **hit** → return cached `Bytes`
   - **miss** → read the full compressed block from storage, decompress, insert into cache
4. Slice the decompressed block bytes at `(offset, size)` — zero-copy via `Bytes::slice()`.
5. Construct the typed array (`PrimitiveArray<T>`, `StringArray`, etc.) from the slice.

For uncompressed blocks (`CodecId::None`), the cache stores the raw bytes directly — no decompression copy. For `PrimitiveArray`, if the slice is already aligned, the typed view is constructed zero-copy from the `Bytes` buffer.

Concurrent requests for the same block are coalesced — only one storage read + decompress happens, and all waiters share the result.

```text
Thread A ----\
Thread B -----+--> [Reader] --> [moka BlockCache]
Thread C ----/             |          |            |
                           |        hit          miss
                           |          |            v
                           |          |   [full-block read via Storage]
                           |          |            |
                           |          |   [decompress (skipped if None)]
                           |          |            |
                           +----------+----> [cache insert]
                                                |
                                                v
                                  [Bytes::slice(offset, size)]
                                                |
                                                v
                                     [construct typed array]
```

### Reader API

```rust
// Open
Reader::open(storage, cache_capacity_bytes) -> Result<Reader>
Reader::open_dyn(Arc<dyn Storage>, cache_capacity_bytes) -> Result<Reader>

// Inspect
reader.footer() -> &Footer
reader.list_arrays() -> Vec<&ArrayMeta>  // non-deleted only

// Typed reads
reader.read_array::<T>(name) -> Result<PrimitiveArray<T>>
reader.read_chunk::<T>(name, coord) -> Result<PrimitiveArray<T>>

// Dynamic reads
reader.read_array_dynamic(name) -> Result<Box<dyn ArrayData>>
reader.read_chunk_dynamic(name, coord) -> Result<Box<dyn ArrayData>>

// Raw bytes
reader.read_raw_bytes(name) -> Result<Bytes>
reader.read_chunk_raw(name, coord) -> Result<Bytes>
```

## Write path

```rust
let config = WriterConfig {
    block_target_size: 8 * 1024 * 1024,  // 8 MiB (default)
    codec: Lz4Codec,                      // or NoCompression, ZstdCodec { level: 3 }
};
let mut writer = Writer::new(storage, config);

// Flat array
writer.write_array("temperatures", vec!["x".into()], &array)?;

// Chunked array
let mut cw = writer.begin_chunked_array(
    "grid", DType::Float32, vec!["x".into(), "y".into()], vec![1000, 1000],
)?;
cw.write_array(vec![0, 0], &chunk_00)?;
cw.write_array(vec![0, 1], &chunk_01)?;
drop(cw);

// Finalize — compresses blocks, writes file
writer.flush().await?;
```

The writer packs array payloads into blocks up to the target size. Each payload is padded to 8-byte alignment within the block. When the current block would exceed the target, it is finalized (compressed) and a new block is started. A flat array is never split — it always occupies a single `ChunkAddress` in one block.

To append to an existing file, use `Writer::open(storage, config)` which reads the existing footer first.

## Compression codecs

Three built-in codecs:

| Type | `CodecId` | Notes |
|------|-----------|-------|
| `NoCompression` | `None` | Pass-through, zero-copy in cache |
| `Lz4Codec` | `Named("lz4")` | Fast compression via `lz4_flex` |
| `ZstdCodec { level }` | `Named("zstd")` | Configurable level (1–22, default 3) |

The codec is set per writer. Each block records its codec in the block table, so the reader infers the correct decompression at read time — no codec parameter is needed when opening a reader.

## Deletes and compaction

Deletes are logical — `Writer::delete(name)` sets the `deleted` flag in the footer. Deleted arrays are excluded from `list_arrays()` and all read operations.

Compaction rewrites the file, keeping only live data:

```rust
compact(&storage, &codec, Some(block_target_size)).await?;
```

```text
Before compact:
  data blocks: [live][deleted][live][deleted]
  footer: marks deleted entries

After compact:
  data blocks: [live][live]
  footer: rewritten without deleted entries
```

## Summary

`array-format` is a block-backed, footer-indexed container for n-dimensional arrays. It supports flat and chunked layouts, fixed-width and variable-length encodings, per-block compression (LZ4, Zstd, or none), zero-copy reads via `Bytes` slicing, a shared `moka` block cache with coalesced concurrent loads, any `object_store`-compatible storage backend, logical deletes, and compaction.
