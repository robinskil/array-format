# array-format

`array-format` stores many n-dimensional arrays in one file.

Instead of putting metadata next to every data chunk, this format writes array bytes first and keeps a footer index at the end of the file. The footer contains the metadata needed to reconstruct arrays and locate their bytes.

## Why this format exists

This format is designed for workloads where you want:

- multiple arrays in one object/file
- either whole-array storage or chunked storage
- append-friendly writes
- efficient lookup via a single footer read
- logical deletes followed by periodic compaction

## Core concepts

### 1) Arrays

Each stored array has a definition in the footer, including:

- data type
- dimensions and dimension names
- layout mode (`flat` or `chunked`)
- references to where the data bytes live

### 2) Blocks

Array bytes are stored in blocks. A block has a configurable target size (default: `8 MiB`).

- one block can hold bytes from multiple arrays/chunks
- one array can span multiple blocks
- if a block has remaining space, more array bytes can be packed into it
- each block may be compressed with a configured compression codec

This packing is useful for storage efficiency and compression behavior.

### 3) Addresses

A chunk is located via a tuple:

`(block_id, offset_in_block, byte_size)`

This means: find block `block_id`, move `offset_in_block` bytes into it, and read `byte_size` bytes.

### 4) Footer index

The footer contains:

- block table: where each block is physically located in the file
- block table also stores block compression metadata (for example codec)
- array table: schema + logical layout + addresses for chunks
- delete flags / logical state for entries

The footer is serialized using `rkyv` for fast decoding from bytes.

All I/O is performed through the `object_store` crate.

## Supported data types and encoding

This section defines how values are encoded in array payloads.

### Type families

The format supports three families of element types:

- fixed-width primitives
- variable-length values (`string`, `binary` / vlen bytes)
- list types (`fixed_size_list`, `list` / vlen list)

Every array entry in the footer should include at least:

- `dtype`: logical type
- `layout`: `flat` or `chunked`
- type-specific parameters (for example `list_size`, child type)

Null values are currently unsupported. Validity bitmaps are not part of this format.

### 1) Fixed-width primitives

Supported primitive scalar types:

- `bool` (1 byte: `0` or `1`)
- signed integers: `int8`, `int16`, `int32`, `int64`
- unsigned integers: `uint8`, `uint16`, `uint32`, `uint64`
- floating point: `float32`, `float64`

Storage rules:

- values are stored contiguously with no per-element header
- integer and float values are encoded little-endian
- element size is constant and known from `dtype`

Example (`int16`, values `[1, 2, 300]`):

```text
01 00 02 00 2C 01
```

### 2) Variable-length values

Supported variable-length scalar types:

- `string` (UTF-8 bytes)
- `binary` (vlen bytes)

Variable-length arrays are stored as three logical buffers:

- offsets buffer (`N + 1` offsets)
- values buffer (concatenated payload bytes)

Recommended offsets type:

- `uint32` offsets when total values buffer size is < 4 GiB
- `uint64` offsets when larger payloads are required

#### Variable-length `string` example

Values: `[
  "cat",
  "",
  "elephant"
]`

```text
offsets (u32): [0, 3, 3, 11]
values bytes : [63 61 74 65 6C 65 70 68 61 6E 74]  // "cat" + "elephant"

index 0 => bytes[0..3]  = "cat"
index 1 => bytes[3..3]  = ""
index 2 => bytes[3..11] = "elephant"
```

#### Variable-length `binary` (vlen byte) example

Values: `[
  [AA BB],
  [01],
  []
]`

```text
offsets (u32): [0, 2, 3, 3]
values bytes : [AA BB 01]

index 0 => bytes[0..2] = [AA BB]
index 1 => bytes[2..3] = [01]
index 2 => bytes[3..3] = []
```

### 3) Fixed-size lists

Supported list type:

- `fixed_size_list<T, K>` where each element is a list of exactly `K` child values of type `T`

Storage rules:

- no offsets buffer is needed because list length is constant (`K`)
- values are stored in a contiguous child buffer

Example (`fixed_size_list<int8, 3>`):

Values: `[[1,2,3], [4,5,6], [7,8,9]]`

```text
child values: [01 02 03 04 05 06 07 08 09]

element 0 => child[0..3]
element 1 => child[3..6]
element 2 => child[6..9]
```

### 4) Variable-length lists (vlen lists)

Supported list type:

- `list<T>` where each element can contain a different number of child values of type `T`

Storage rules:

- parent offsets buffer has `N + 1` entries
- child values buffer stores all child elements concatenated
- each list element `i` is `child[offsets[i]..offsets[i+1]]`

Example (`list<int16>`):

Values: `[[10,20,30], [], [40]]`

```text
parent offsets (u32): [0, 3, 3, 4]
child values (int16 LE): [0A 00 14 00 1E 00 28 00]

element 0 => child[0..3] = [10,20,30]
element 1 => child[3..3] = []
element 2 => child[3..4] = [40]
```

Nested variable-length example (`list<string>`):

```text
parent offsets (lists): [0, 2, 3]
child string offsets  : [0, 1, 3, 6]
child string values   : [61 62 63 64 65 66]  // "a", "bc", "def"

list 0 uses strings 0..2 => ["a", "bc"]
list 1 uses strings 2..3 => ["def"]
```

This means vlen list nesting introduces one offsets buffer per variable-length level.

### Nullability

Nullability is currently unsupported for all types.

- validity bitmaps are not stored
- null values cannot be represented in payloads
- empty string/empty binary/empty list are valid values, but they are not null

### Chunk interaction

For chunked arrays, each chunk is encoded with the same dtype rules as above. Metadata maps chunk coordinates to chunk addresses; each addressed payload then contains the encoded bytes for that chunk.

## On-disk layout

The high-level file structure looks like this:

```text
+-------------------------------+ 0
| Data Region                   |
|                               |
|  [block 0 bytes]              |
|  [block 1 bytes]              |
|  [block 2 bytes]              |
|  ...                          |
+-------------------------------+
| Footer (index + metadata)     |
+-------------------------------+ EOF
```

Inside the data region, blocks may contain mixed content:

```text
block 12 (example)

+---------------------------------------------------------+
| arrA chunk0 | arrB chunk9 | arrC flat data slice | ... |
+---------------------------------------------------------+
0             ^             ^                        ^
              offsets tracked in footer addresses
```

## Flat vs chunked layout

### Flat layout

The array is stored as one logical payload (possibly split physically by block boundaries).

```text
Array X (flat)
  logical payload ------------------------------------------
  address list: [ (b1,o0,s4MB), (b1,o4MB,s4MB), (b2,o0,s2MB) ]
```

### Chunked layout

The array is divided into chunk coordinates, and each chunk has one or more addresses.

```text
Array Y (chunked, 2D example)

Logical chunk grid:

  +--------+--------+
  | (0,0)  | (0,1)  |
  +--------+--------+
  | (1,0)  | (1,1)  |
  +--------+--------+

Footer mapping (example):
  (0,0) -> (b3,o0,1MB)
  (0,1) -> (b7,o2MB,1MB)
  (1,0) -> (b3,o1MB,1MB)
  (1,1) -> (b9,o0,1MB)
```

## Read path

To read an array:

1. Read footer from the end of file.
2. Resolve array metadata by array id/name.
3. Collect its address list (flat or per-chunk).
4. For each referenced block, check the reader-owned shared `moka` cache of decompressed blocks.
5. On cache miss, read the entire block from storage using `object_store`.
6. Decompress the entire block in memory based on block codec metadata.
7. Store decompressed block bytes in the shared `moka` cache.
8. Slice chunk bytes by `(block_id, offset, size)` from decompressed block memory.
9. Decode and reconstruct requested array/chunks.

Read behavior is block-granular:

- reads never perform partial block I/O from storage
- a miss always reads one full block and decompresses one full block
- cache capacity is configurable
- cached blocks are shareable across threads using the same reader
- the read path is parallel-safe via the `moka` cache

### Shared reader and cache concurrency model

Each reader instance owns a single shared `moka` cache that proxies block I/O requests.

- all read calls through that reader consult the same cache instance
- cache hit: return decompressed block bytes from cache memory
- cache miss: perform full-block read via `object_store`, decompress, then insert into the same cache
- many threads can call the same reader concurrently
- threads may read arrays from the same block or different blocks at the same time
- when threads converge on the same block, cache reuse prevents repeated full-block I/O/decompression after the block is cached

```text
Thread A ----\
Thread B -----+--> [Reader] --> [Single shared moka cache]
Thread C ----/             |             |            |
                           |           hit          miss
                           |             |            v
                           |             |   [object_store full-block read]
                           |             |            |
                           |             |   [full-block decompress]
                           |             |            |
                           +-------------+----> [cache insert]
                                                 |
                                                 v
                                   [slice chunk bytes and decode]
```

```text
Reader Task
  |
  v
[Read footer] --> [Resolve chunk addresses]
                |
                v
          [For each referenced block]
                |
                v
          [Lookup block in moka cache]
              |                 |
            hit|                 |miss
              v                 v
        [Use cached bytes]   [Read FULL block via object_store]
              |                 |
              |                 v
              |         [Decompress FULL block in memory]
              |                 |
              |                 v
              |         [Insert into shared moka cache]
              |                 |
              +--------+--------+
                    |
                    v
         [Slice (offset,size) from decompressed block]
                    |
                    v
            [Decode chunk(s) and reconstruct array]
```

## Write path

To write array data:

1. Encode array bytes (flat payload or chunk payloads).
2. Append bytes into one or more blocks in data region.
3. Record new addresses for each payload/chunk.
4. Append/update footer metadata with new array entry and block table state.

Because the index lives in the footer, data writes can remain append-oriented.

## Deletes and compaction

Deletes are logical, not immediate physical removal.

- delete operation: mark array/chunk metadata as deleted
- read behavior: ignore logically deleted entries
- compaction: rewrite live data + rebuild footer without deleted entries

```text
Before compact:
  data blocks: [live][deleted][live][deleted]
  footer: marks deleted entries

After compact:
  data blocks: [live][live]
  footer: rewritten without deleted entries
```

## Footer anatomy (conceptual)

```text
Footer
+------------------------------------------------------+
| Footer header                                        |
|  - version                                           |
|  - counts / offsets                                  |
+------------------------------------------------------+
| Block table                                          |
|  - block_id -> file_position, compressed_size, ...   |
|  - compression codec metadata                        |
+------------------------------------------------------+
| Array table                                          |
|  - array_id/name                                     |
|  - dtype, dims, dim_names                            |
|  - layout mode (flat/chunked)                        |
|  - chunk/address mapping                             |
|  - deleted flag                                      |
+------------------------------------------------------+
```

## Summary

`array-format` is a block-backed, footer-indexed container for n-dimensional arrays. It supports flat and chunked layouts, fixed-width and variable-width element encodings (including vlen lists), block compression codecs, tuple-based address lookup, shared configurable `moka` block caching for parallel-safe reads, logical deletes, and compaction-based physical cleanup.
