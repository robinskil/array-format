//! Compaction: rewrite a file to remove logically deleted arrays.
//!
//! Compaction reads the existing footer, drops deleted arrays, repacks
//! live data into fresh blocks, and writes a new file.

use bytes::Bytes;

use crate::address::{BlockId, ChunkAddress};
use crate::block::BlockMeta;
use crate::codec::{CompressionCodec, decompress_by_id};
use crate::error::Result;
use crate::footer::Footer;
use crate::layout::{ArrayLayout, ArrayMeta, StorageLayout};
use crate::storage::Storage;
use crate::writer::DEFAULT_BLOCK_TARGET_SIZE;

/// Compacts a file by removing logically deleted arrays and repacking blocks.
///
/// Reads the entire file via `storage`, filters out deleted arrays,
/// repacks the live data into fresh blocks (compressed with `codec`),
/// and overwrites the file with the compacted content.
///
/// The decompression codec for existing blocks is inferred from the
/// block metadata. `codec` is used for compressing new blocks.
///
/// `block_target_size` controls the target uncompressed size of each new block.
pub async fn compact(
    storage: &(dyn Storage + Sync),
    codec: &dyn CompressionCodec,
    block_target_size: Option<usize>,
) -> Result<()> {
    let block_target = block_target_size.unwrap_or(DEFAULT_BLOCK_TARGET_SIZE);

    // 1. Read existing footer.
    let old_footer = crate::footer::read_footer(storage).await?;

    // 2. Collect live arrays and read their data.
    let live_arrays: Vec<&ArrayMeta> = old_footer.arrays.iter().filter(|a| !a.deleted).collect();

    struct ArrayData {
        meta: ArrayMeta,
        payload: ArrayPayload,
    }

    enum ArrayPayload {
        Flat(Vec<u8>),
        Chunked {
            chunk_shape: Vec<u32>,
            chunks: Vec<(Vec<u32>, Vec<u8>)>,
        },
    }

    let mut array_data_vec: Vec<ArrayData> = Vec::new();

    for array in &live_arrays {
        let payload = match &array.layout.storage {
            StorageLayout::Flat { address } => {
                let block_bytes =
                    read_and_decompress_block(storage, &old_footer, address.block_id).await?;
                let start = address.offset as usize;
                let end = start + address.size as usize;
                ArrayPayload::Flat(block_bytes[start..end].to_vec())
            }
            StorageLayout::Chunked {
                chunk_shape,
                chunks,
            } => {
                let mut chunk_data = Vec::new();
                for (coord, addr) in chunks {
                    let block_bytes =
                        read_and_decompress_block(storage, &old_footer, addr.block_id).await?;
                    let start = addr.offset as usize;
                    let end = start + addr.size as usize;
                    chunk_data.push((coord.clone(), block_bytes[start..end].to_vec()));
                }
                ArrayPayload::Chunked {
                    chunk_shape: chunk_shape.clone(),
                    chunks: chunk_data,
                }
            }
        };

        array_data_vec.push(ArrayData {
            meta: ArrayMeta {
                name: array.name.clone(),
                dtype: array.dtype.clone(),
                layout: ArrayLayout {
                    shape: array.layout.shape.clone(),
                    dimension_names: array.layout.dimension_names.clone(),
                    storage: StorageLayout::Flat {
                        address: ChunkAddress {
                            block_id: BlockId(0),
                            offset: 0,
                            size: 0,
                        },
                    }, // placeholder
                },
                fill_value: array.fill_value.clone(),
                deleted: false,
            },
            payload,
        });
    }

    // 3. Repack into new blocks.
    let mut new_blocks: Vec<(BlockMeta, Vec<u8>)> = Vec::new();
    let mut current_block: Vec<u8> = Vec::new();
    let mut new_arrays: Vec<ArrayMeta> = Vec::new();

    let mut next_block_id = 0u32;

    let finalize_block = |current: &mut Vec<u8>,
                          blocks: &mut Vec<(BlockMeta, Vec<u8>)>,
                          id: &mut u32|
     -> Result<()> {
        if current.is_empty() {
            return Ok(());
        }
        let uncompressed = std::mem::take(current);
        let uncompressed_size = uncompressed.len() as u64;
        let compressed = codec.compress(&uncompressed)?;
        let meta = BlockMeta {
            id: BlockId(*id),
            file_offset: 0,
            compressed_size: compressed.len() as u64,
            uncompressed_size,
            codec: codec.id(),
        };
        blocks.push((meta, compressed));
        *id += 1;
        Ok(())
    };

    for mut ad in array_data_vec {
        match ad.payload {
            ArrayPayload::Flat(data) => {
                let address = pack_data(
                    &data,
                    block_target,
                    &mut current_block,
                    &mut new_blocks,
                    &mut next_block_id,
                    codec,
                )?;
                ad.meta.layout.storage = StorageLayout::Flat { address };
            }
            ArrayPayload::Chunked {
                chunk_shape,
                chunks,
            } => {
                let mut chunk_entries = Vec::new();
                for (coord, data) in chunks {
                    let addr = pack_data(
                        &data,
                        block_target,
                        &mut current_block,
                        &mut new_blocks,
                        &mut next_block_id,
                        codec,
                    )?;
                    chunk_entries.push((coord, addr));
                }
                ad.meta.layout.storage = StorageLayout::Chunked {
                    chunk_shape,
                    chunks: chunk_entries,
                };
            }
        }
        new_arrays.push(ad.meta);
    }

    // Finalize remaining data in current block.
    finalize_block(&mut current_block, &mut new_blocks, &mut next_block_id)?;

    // 4. Write new file.
    let mut file_bytes: Vec<u8> = Vec::new();
    let mut final_block_metas: Vec<BlockMeta> = Vec::new();
    for (mut meta, compressed) in new_blocks {
        meta.file_offset = file_bytes.len() as u64;
        file_bytes.extend_from_slice(&compressed);
        final_block_metas.push(meta);
    }

    let new_footer = Footer {
        version: crate::footer::FOOTER_VERSION,
        blocks: final_block_metas,
        arrays: new_arrays,
    };
    let footer_bytes = new_footer.serialize()?;
    file_bytes.extend_from_slice(&footer_bytes);

    storage.write(Bytes::from(file_bytes)).await?;
    Ok(())
}

async fn read_and_decompress_block(
    storage: &(dyn Storage + Sync),
    footer: &Footer,
    block_id: BlockId,
) -> Result<Vec<u8>> {
    let block_meta = footer.blocks.iter().find(|b| b.id == block_id).ok_or(
        crate::error::Error::BlockOutOfRange {
            block_id: block_id.0,
        },
    )?;
    let compressed = storage.read_range(block_meta.file_range()).await?;
    decompress_by_id(
        &block_meta.codec,
        &compressed,
        block_meta.uncompressed_size as usize,
    )
}

fn pack_data(
    data: &[u8],
    block_target: usize,
    current_block: &mut Vec<u8>,
    finished_blocks: &mut Vec<(BlockMeta, Vec<u8>)>,
    next_id: &mut u32,
    codec: &dyn CompressionCodec,
) -> Result<ChunkAddress> {
    // Flush current block if appending would exceed the target and it's non-empty.
    if !current_block.is_empty() && current_block.len() + data.len() > block_target {
        let uncompressed = std::mem::take(current_block);
        let uncompressed_size = uncompressed.len() as u64;
        let compressed = codec.compress(&uncompressed)?;
        let meta = BlockMeta {
            id: BlockId(*next_id),
            file_offset: 0,
            compressed_size: compressed.len() as u64,
            uncompressed_size,
            codec: codec.id(),
        };
        finished_blocks.push((meta, compressed));
        *next_id += 1;
    }

    let offset = current_block.len() as u32;
    current_block.extend_from_slice(data);
    let block_id = BlockId(*next_id);

    Ok(ChunkAddress {
        block_id,
        offset,
        size: data.len() as u32,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::NoCompression;
    use crate::dtype::DType;
    use crate::reader::Reader;
    use crate::storage::InMemoryStorage;
    use crate::writer::{Writer, WriterConfig};

    fn small_config() -> WriterConfig<NoCompression> {
        WriterConfig {
            block_target_size: 64,
            codec: NoCompression,
        }
    }

    #[tokio::test]
    async fn compact_removes_deleted() {
        let storage = InMemoryStorage::new();

        // Write 3 arrays, delete one.
        {
            let mut writer = Writer::new(storage.clone(), small_config());
            writer
                .write_flat("a", DType::UInt8, vec![], vec![], None, &[1; 20])
                .unwrap();
            writer
                .write_flat("b", DType::UInt8, vec![], vec![], None, &[2; 20])
                .unwrap();
            writer
                .write_flat("c", DType::UInt8, vec![], vec![], None, &[3; 20])
                .unwrap();
            writer.delete("b").unwrap();
            writer.flush().await.unwrap();
        }

        // Compact.
        compact(&storage, &NoCompression, Some(64)).await.unwrap();

        // Verify only a and c remain.
        let reader = Reader::open(storage, 1024).await.unwrap();
        let mut names: Vec<_> = reader
            .list_arrays()
            .iter()
            .map(|a| a.name.clone())
            .collect();
        names.sort();
        assert_eq!(names, vec!["a", "c"]);

        // Verify data integrity.
        let a = reader.read_array::<u8>("a").await.unwrap();
        assert_eq!(a.values(), &[1u8; 20]);
        let c = reader.read_array::<u8>("c").await.unwrap();
        assert_eq!(c.values(), &[3u8; 20]);

        // b should not be found.
        assert!(reader.read_array::<u8>("b").await.is_err());
    }

    #[tokio::test]
    async fn compact_preserves_all_when_no_deletes() {
        let storage = InMemoryStorage::new();
        {
            let mut writer = Writer::new(storage.clone(), small_config());
            writer
                .write_flat("x", DType::Float64, vec!["t".into()], vec![5], None, &[0xFF; 40])
                .unwrap();
            writer.flush().await.unwrap();
        }

        compact(&storage, &NoCompression, Some(64)).await.unwrap();

        let reader = Reader::open(storage, 1024).await.unwrap();
        assert_eq!(reader.list_arrays().len(), 1);
        let x = reader.read_raw_bytes("x").await.unwrap();
        assert_eq!(x, vec![0xFFu8; 40]);
    }
}
