use std::sync::Arc;

use bytes::Bytes;

use crate::{
    address::{BlockAllocAddress, BlockId},
    block::BlockMeta,
    codec::{CompressionCodec, decompress_by_id},
};

#[cfg(unix)]
fn read_exact_at(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

#[cfg(windows)]
fn read_exact_at(
    file: &std::fs::File,
    mut buf: &mut [u8],
    mut offset: u64,
) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_read(buf, offset) {
            Ok(0) => {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            Ok(n) => {
                let tmp = buf;
                buf = &mut tmp[n..];
                offset += n as u64;
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Packs raw chunk bytes into compressed blocks.
///
/// The current (unflushed) block is held in memory; completed blocks are
/// compressed and written to a single backing tempfile. [`fetch`](DeltaAllocator::fetch)
/// serves the current block from memory and decompresses completed blocks
/// from that file on demand — no second file is needed.
///
/// Call [`commit`](DeltaAllocator::commit) to flush the final block and
/// retrieve the compressed output file plus block metadata.
pub struct DeltaAllocator {
    codec: Arc<dyn CompressionCodec>,
    block_target_size: usize,
    // Current (not-yet-flushed) block — kept in memory for cheap fetch().
    current_block_id: u32,
    current_block: Vec<u8>,
    // Single file: compressed blocks are appended here on each flush.
    // Completed-block fetches decompress directly from this file.
    output_file: std::fs::File,
    completed_blocks: Vec<BlockMeta>,
    file_offset: u64,
}

/// The output of [`DeltaAllocator::commit`]: a file handle to the compressed
/// block bytes (seeked to the start), the total byte count, and the block
/// metadata table.
pub struct AllocatorOutput {
    /// Compressed block data, seeked to position 0 and ready to read.
    pub file: tokio::fs::File,
    /// Total number of bytes in `file`.
    pub output_size: u64,
    pub blocks: Vec<BlockMeta>,
}

impl DeltaAllocator {
    pub fn new(codec: Arc<dyn CompressionCodec>, block_target_size: usize) -> Self {
        let output_file =
            tempfile::tempfile().expect("DeltaAllocator: failed to create output tempfile");
        Self {
            codec,
            block_target_size,
            current_block_id: 0,
            current_block: Vec::new(),
            output_file,
            completed_blocks: Vec::new(),
            file_offset: 0,
        }
    }

    /// Appends `slice` to the current block and returns its allocation address.
    /// Flushes the current block to the output file if it reaches the target size.
    pub fn allocate(&mut self, slice: &[u8]) -> BlockAllocAddress {
        let block_id = self.current_block_id;
        let intra_offset = self.current_block.len() as u64;
        self.current_block.extend_from_slice(slice);
        if self.current_block.len() >= self.block_target_size {
            self.flush_block();
        }
        BlockAllocAddress::new(BlockId(block_id), intra_offset, slice.len() as u64)
    }

    fn flush_block(&mut self) {
        use std::io::Write;
        if self.current_block.is_empty() {
            return;
        }
        let uncompressed_size = self.current_block.len() as u64;
        let compressed = self
            .codec
            .compress(&self.current_block)
            .expect("compression failed");
        let compressed_size = compressed.len() as u64;
        self.completed_blocks.push(BlockMeta {
            id: BlockId(self.current_block_id),
            file_offset: self.file_offset,
            compressed_size,
            uncompressed_size,
            codec: self.codec.id(),
        });
        self.output_file.write_all(&compressed).expect("output_file write failed");
        self.file_offset += compressed_size;
        self.current_block_id += 1;
        self.current_block.clear();
    }

    /// Returns the raw (uncompressed) bytes for the given allocation address.
    ///
    /// Serves the current block from memory; for completed blocks it reads
    /// from the tempfile at the block's offset (positional read, no seek),
    /// decompresses the block, and slices out the bytes.
    pub fn fetch(&self, addr: &BlockAllocAddress) -> Option<Bytes> {
        let block_id = addr.id().0;
        let off = addr.offset() as usize;
        let sz = addr.size() as usize;

        if block_id == self.current_block_id {
            let end = off + sz;
            if end > self.current_block.len() {
                return None;
            }
            return Some(Bytes::copy_from_slice(&self.current_block[off..end]));
        }

        let block = self.completed_blocks.iter().find(|b| b.id.0 == block_id)?;
        let file_offset = block.file_offset;
        let compressed_size = block.compressed_size as usize;
        let uncompressed_size = block.uncompressed_size as usize;
        let codec = block.codec.clone();

        let mut compressed = vec![0u8; compressed_size];
        read_exact_at(&self.output_file, &mut compressed, file_offset).ok()?;

        let decompressed = decompress_by_id(&codec, &compressed, uncompressed_size).ok()?;
        let end = off + sz;
        Some(Bytes::copy_from_slice(&decompressed[off..end]))
    }

    /// Flushes the remaining partial block and returns a file handle to the
    /// compressed output (seeked to position 0), the total output size, and
    /// the block metadata table.
    pub async fn commit(mut self) -> AllocatorOutput {
        use tokio::io::AsyncSeekExt;
        self.flush_block();
        let output_size = self.file_offset;
        let mut file = tokio::fs::File::from_std(self.output_file);
        file.seek(std::io::SeekFrom::Start(0))
            .await
            .expect("output_file seek failed");
        AllocatorOutput { file, output_size, blocks: self.completed_blocks }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{NoCompression, codec::CompressionCodec};

    fn codec() -> Arc<dyn CompressionCodec> {
        Arc::new(NoCompression)
    }

    #[test]
    fn allocator_address_reflects_block_and_offset() {
        let mut alloc = DeltaAllocator::new(codec(), 1024);
        let a = alloc.allocate(&[1, 2, 3, 4]);
        assert_eq!(a.id(), BlockId(0));
        assert_eq!(a.offset(), 0);
        assert_eq!(a.size(), 4);

        let b = alloc.allocate(&[5, 6]);
        assert_eq!(b.id(), BlockId(0));
        assert_eq!(b.offset(), 4);
        assert_eq!(b.size(), 2);
    }

    #[test]
    fn allocator_fetch_from_current_block() {
        let mut alloc = DeltaAllocator::new(codec(), 1024);
        let addr = alloc.allocate(&[10, 20, 30]);
        let bytes = alloc.fetch(&addr).expect("fetch returned None");
        assert_eq!(bytes.as_ref(), &[10, 20, 30]);
    }

    #[test]
    fn allocator_fetch_second_slice_in_current_block() {
        let mut alloc = DeltaAllocator::new(codec(), 1024);
        alloc.allocate(&[0u8; 8]);
        let addr = alloc.allocate(&[42, 43, 44, 45]);
        let bytes = alloc.fetch(&addr).unwrap();
        assert_eq!(bytes.as_ref(), &[42, 43, 44, 45]);
    }

    #[test]
    fn allocator_flush_triggered_at_target_size() {
        let mut alloc = DeltaAllocator::new(codec(), 8);
        let addr = alloc.allocate(&[1u8; 8]);
        assert_eq!(alloc.current_block_id, 1, "expected flush to advance block_id");
        assert_eq!(addr.id(), BlockId(0));
    }

    #[test]
    fn allocator_fetch_from_completed_block() {
        let payload = [0xABu8; 8];
        let mut alloc = DeltaAllocator::new(codec(), 8);
        let addr = alloc.allocate(&payload);
        alloc.allocate(&[0u8; 4]);
        let bytes = alloc.fetch(&addr).expect("fetch from completed block returned None");
        assert_eq!(bytes.as_ref(), &payload);
    }

    #[tokio::test]
    async fn allocator_commit_captures_all_blocks() {
        let mut alloc = DeltaAllocator::new(codec(), 8);
        alloc.allocate(&[1u8; 8]); // fills block 0 → flush
        alloc.allocate(&[2u8; 6]); // partial block 1

        let out = alloc.commit().await;
        assert_eq!(out.blocks.len(), 2);
        assert_eq!(out.blocks[0].id, BlockId(0));
        assert_eq!(out.blocks[1].id, BlockId(1));
        assert_eq!(out.blocks[0].uncompressed_size, 8);
        assert_eq!(out.blocks[1].uncompressed_size, 6);
    }

    #[tokio::test]
    async fn allocator_commit_output_decompresses_to_original() {
        let data: Vec<u8> = (0u8..=127).collect();
        let mut alloc = DeltaAllocator::new(codec(), 64);
        let addr = alloc.allocate(&data[..64]); // block 0
        alloc.allocate(&data[64..]); // block 1

        let mut out = alloc.commit().await;

        use tokio::io::AsyncReadExt;
        let mut all_bytes = vec![0u8; out.output_size as usize];
        out.file.read_exact(&mut all_bytes).await.unwrap();

        let b0 = &out.blocks[0];
        let raw0: Vec<u8> = crate::codec::decompress_by_id(
            &b0.codec,
            &all_bytes[..b0.compressed_size as usize],
            b0.uncompressed_size as usize,
        )
        .unwrap()
        .to_vec();
        assert_eq!(raw0, &data[..64]);

        let off1 = b0.compressed_size as usize;
        let b1 = &out.blocks[1];
        let raw1: Vec<u8> = crate::codec::decompress_by_id(
            &b1.codec,
            &all_bytes[off1..],
            b1.uncompressed_size as usize,
        )
        .unwrap()
        .to_vec();
        assert_eq!(raw1, &data[64..]);

        assert_eq!(addr.id(), BlockId(0));
        assert_eq!(addr.size() as usize, 64);
    }
}
